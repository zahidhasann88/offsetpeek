package kafkax

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/zahidhasann88/offsetpeek/internal/admin"
	"github.com/zahidhasann88/offsetpeek/internal/preview"
	"github.com/zahidhasann88/offsetpeek/internal/reset"
)

func TestSmoke_RealKadmAgainstKfakeCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cluster, err := kfake.NewCluster()
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}
	defer cluster.Close()
	seed := cluster.ListenAddrs()[0]
	t.Logf("seed: %s", seed)

	setupCl, err := kgo.NewClient(kgo.SeedBrokers(seed), kgo.ClientID("op-setup"))
	if err != nil {
		t.Fatalf("setup client: %v", err)
	}
	defer setupCl.Close()
	setupAdm := kadm.NewClient(setupCl)
	if _, err := setupAdm.CreateTopic(ctx, 5, 1, nil, "orders"); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for i := 0; i < 5; i++ {
		setupCl.Produce(ctx, &kgo.Record{Topic: "orders", Value: []byte("hi")}, nil)
	}
	if err := setupCl.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	cl, kc, err := NewClient(Config{Brokers: []string{seed}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()
	a := admin.NewKadmClient(kc)
	defer a.Close()

	t.Run("new_group", func(t *testing.T) {
		r, err := preview.Run(ctx, a, preview.Options{
			Brokers: seed,
			Group:   "smoketest-empty",
			Topic:   "orders",
			Target:  reset.Target{Kind: reset.KindEarliest},
		})
		if err != nil {
			t.Fatalf("preview.Run: %v", err)
		}
		if !r.WasDryRun {
			t.Fatal("WasDryRun must be true")
		}
		if !r.NewGroup {
			t.Fatal("expected NewGroup=true for unknown group")
		}
		if len(r.Partitions) != 5 {
			t.Fatalf("want all 5 partitions of orders as NEW, got %d", len(r.Partitions))
		}
		for _, p := range r.Partitions {
			if p.Verdict != reset.VerdictNew {
				t.Fatalf("want NEW, got %+v", p)
			}
		}
	})

	t.Run("with_committed_offset", func(t *testing.T) {
		var off kadm.Offsets
		off.AddOffset("orders", 0, 3, -1)
		resp, err := setupAdm.CommitOffsets(ctx, "smoketest-existing", off)
		if err != nil {
			t.Fatalf("setup commit: %v", err)
		}
		t.Logf("commit resp: %+v", resp)

		r, err := preview.Run(ctx, a, preview.Options{
			Brokers: seed,
			Group:   "smoketest-existing",
			Topic:   "orders",
			Target:  reset.Target{Kind: reset.KindEarliest},
		})
		if err != nil {
			t.Fatalf("preview.Run: %v", err)
		}
		if r.NewGroup {
			t.Fatalf("expected NewGroup=false after committing; partitions=%d", len(r.Partitions))
		}
		if len(r.Partitions) == 0 {
			t.Fatalf("expected partitions; got 0")
		}
		p := r.Partitions[0]
		if p.Verdict != reset.VerdictReprocess {
			t.Fatalf("verdict want REPROCESS got %s (%+v)", p.Verdict, p)
		}
		if p.Delta != -3 {
			t.Fatalf("delta want -3 got %d", p.Delta)
		}
	})
}
