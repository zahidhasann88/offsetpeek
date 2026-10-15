//go:build integration

// Package testutil holds the real-Kafka integration test (testcontainers-go).
// Enable with -tags=integration; skip with -short.
package testutil

import (
	"context"
	"strings"
	"testing"
	"time"

	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/example/offsetpeek/internal/admin"
	"github.com/example/offsetpeek/internal/kafkax"
	"github.com/example/offsetpeek/internal/preview"
	"github.com/example/offsetpeek/internal/reset"
)

const kafkaImage = "confluentinc/confluent-local:7.5.0"

func TestIntegration_RealKafka(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-Kafka integration test in -short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Log("starting kafka container…")
	kc, err := tckafka.Run(ctx, kafkaImage, tckafka.WithClusterID("offsetpeek-test"))
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("docker unavailable: %v", err)
		}
		// On Docker Desktop for Windows, try TESTCONTAINERS_RYUK_DISABLED=true.
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = kc.Terminate(stopCtx)
	})
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("brokers: %v", err)
	}
	t.Logf("kafka brokers: %v", brokers)

	const (
		topic = "orders"
		group = "payments"
		n     = 100
	)

	setup, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("offsetpeek-it-setup"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("setup client: %v", err)
	}
	defer setup.Close()
	setupAdm := kadm.NewClient(setup)
	if _, err := setupAdm.CreateTopic(ctx, 2, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	value := []byte(strings.Repeat("x", 100))
	for i := 0; i < n; i++ {
		r := &kgo.Record{Topic: topic, Partition: int32(i % 2), Value: value}
		if err := setup.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	var off kadm.Offsets
	off.AddOffset(topic, 0, 40, -1) // 50 messages in p0
	off.AddOffset(topic, 1, 50, -1) // 50 messages in p1, fully consumed
	if _, err := setupAdm.CommitOffsets(ctx, group, off); err != nil {
		t.Fatalf("commit (setup): %v", err)
	}

	cl, _, err := kafkax.NewClient(kafkax.Config{Brokers: brokers})
	if err != nil {
		t.Fatalf("kafkax.NewClient: %v", err)
	}
	defer cl.Close()
	a := kafkax.Wrap(cl)

	run := func(tgt reset.Target) map[int32]reset.PerPartitionResult {
		t.Helper()
		report, err := preview.Run(ctx, a, preview.Options{
			Brokers: brokers[0], Group: group, Topic: topic, Target: tgt, SampleRecords: 10,
		})
		if err != nil {
			t.Fatalf("preview.Run(%s): %v", tgt.Kind, err)
		}
		if len(report.Warnings) > 0 {
			t.Fatalf("unexpected warnings: %v", report.Warnings)
		}
		rows := map[int32]reset.PerPartitionResult{}
		for _, p := range report.Partitions {
			rows[p.Partition] = p
		}
		return rows
	}

	t.Run("earliest", func(t *testing.T) {
		rows := run(reset.Target{Kind: reset.KindEarliest})
		if p := rows[0]; p.Verdict != reset.VerdictReprocess || p.Delta != -40 {
			t.Fatalf("p0: %+v", p)
		}
		if p := rows[1]; p.Verdict != reset.VerdictReprocess || p.Delta != -50 {
			t.Fatalf("p1: %+v", p)
		}
		if p := rows[0]; p.AvgRecordBytes != 100 || p.EstBytes != 4000 {
			t.Fatalf("sampling p0: avg=%v est=%d", p.AvgRecordBytes, p.EstBytes)
		}
	})
	t.Run("latest", func(t *testing.T) {
		rows := run(reset.Target{Kind: reset.KindLatest})
		if p := rows[0]; p.Verdict != reset.VerdictSkip || p.Delta != 10 {
			t.Fatalf("p0: %+v", p)
		}
		if p := rows[1]; p.Verdict != reset.VerdictNoChange {
			t.Fatalf("p1: %+v", p)
		}
	})
	t.Run("time_in_future_resolves_to_latest", func(t *testing.T) {
		rows := run(reset.Target{Kind: reset.KindTime, Value: time.Now().Add(time.Hour)})
		if p := rows[0]; p.Verdict != reset.VerdictSkip || p.Target != 50 {
			t.Fatalf("p0: %+v", p)
		}
	})
	t.Run("time_in_past_resolves_to_earliest", func(t *testing.T) {
		rows := run(reset.Target{Kind: reset.KindTime, Value: time.Unix(0, 0)})
		if p := rows[0]; p.Verdict != reset.VerdictReprocess || p.Target != 0 {
			t.Fatalf("p0: %+v", p)
		}
	})

	fetched, err := a.FetchOffsets(ctx, group, topic)
	if err != nil {
		t.Fatalf("re-fetch: %v", err)
	}
	want := map[int32]int64{0: 40, 1: 50}
	for _, p := range fetched {
		if p.Committed != want[p.Partition] {
			t.Fatalf("committed offset changed! %s/%d want=%d got=%d",
				p.Topic, p.Partition, want[p.Partition], p.Committed)
		}
	}
	var _ admin.AdminClient = a
}

func isDockerUnavailable(err error) bool {
	msg := err.Error()
	for _, s := range []string{
		"Cannot connect to the Docker daemon",
		"docker.sock", "docker daemon", "no such host",
		"rootless Docker not found", "failed to create Docker provider",
		"docker_engine", // Windows named pipe
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
