// Command offsetpeek-sample runs offsetpeek against an in-memory Kafka
// (kfake) with seeded data. A development demo; no Docker needed.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/zahidhasann88/offsetpeek/internal/admin"
	"github.com/zahidhasann88/offsetpeek/internal/kafkax"
	"github.com/zahidhasann88/offsetpeek/internal/preview"
	"github.com/zahidhasann88/offsetpeek/internal/render"
	"github.com/zahidhasann88/offsetpeek/internal/reset"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "demo error:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cluster, err := kfake.NewCluster()
	if err != nil {
		return fmt.Errorf("kfake: %w", err)
	}
	defer cluster.Close()
	seed := cluster.ListenAddrs()[0]

	prod, err := kgo.NewClient(kgo.SeedBrokers(seed), kgo.ClientID("demo-prod"))
	if err != nil {
		return err
	}
	defer prod.Close()

	adm := kadm.NewClient(prod)
	if _, err := adm.CreateTopic(ctx, 4, 1, nil, "orders"); err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	for i := 0; i < 3000; i++ {
		prod.Produce(ctx, &kgo.Record{
			Topic: "orders",
			Key:   []byte(fmt.Sprintf("user-%d", i%50)),
			Value: []byte(fmt.Sprintf(`{"order_id":%d}`, i)),
		}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	var off kadm.Offsets
	off.AddOffset("orders", 0, 420, -1)
	off.AddOffset("orders", 1, 380, -1)
	off.AddOffset("orders", 2, 510, -1)
	off.AddOffset("orders", 3, 290, -1)
	if _, err := adm.CommitOffsets(ctx, "payments", off); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	kgoCl, kadmCl, err := kafkax.NewClient(kafkax.Config{Brokers: []string{seed}})
	if err != nil {
		return err
	}
	defer kgoCl.Close()
	a := admin.NewKadmClient(kadmCl).WithSampling(kgoCl)

	specs := []struct {
		label string
		tgt   reset.Target
	}{
		{"--to-earliest --sample 100", reset.Target{Kind: reset.KindEarliest}},
		{"--shift-by -1000 --sample 100", reset.Target{Kind: reset.KindShift, Shift: -1000}},
		{"--to-time -1m --sample 100", reset.Target{Kind: reset.KindTime, Value: time.Now().Add(-time.Minute)}},
		{"--to-offset orders:0=0 --to-offset orders:1=500 --sample 100", reset.Target{
			Kind:    reset.KindOffset,
			Offsets: []reset.PartitionOffset{{Topic: "orders", Partition: 0, Offset: 0}, {Topic: "orders", Partition: 1, Offset: 500}},
		}},
	}
	for _, s := range specs {
		fmt.Printf("\n=== %s\n\n", s.label)
		report, err := preview.Run(ctx, a, preview.Options{
			Brokers:       seed,
			Group:         "payments",
			Topic:         "orders",
			Target:        s.tgt,
			SampleRecords: 100,
		})
		if err != nil {
			return fmt.Errorf("preview: %w", err)
		}
		if err := render.Table(report, os.Stdout); err != nil {
			return err
		}
	}
	return nil
}
