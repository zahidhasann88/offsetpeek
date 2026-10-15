package preview

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/example/offsetpeek/internal/admin"
	"github.com/example/offsetpeek/internal/reset"
)

func TestRun_HappyEarliest(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "orders", Partition: 0, Committed: 1500},
			{Topic: "orders", Partition: 1, Committed: 200},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 10_000},
			{Topic: "orders", Partition: 1, Earliest: 0, Latest: 10_000},
		},
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers: "localhost:9092",
		Group:   "payments",
		Target:  reset.Target{Kind: reset.KindEarliest},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Partitions) != 2 {
		t.Fatalf("parts: %d", len(report.Partitions))
	}
	if report.Partitions[0].Verdict != reset.VerdictReprocess || report.Partitions[0].Delta != -1500 {
		t.Fatalf("p0: %+v", report.Partitions[0])
	}
	if report.Partitions[1].Verdict != reset.VerdictReprocess || report.Partitions[1].Delta != -200 {
		t.Fatalf("p1: %+v", report.Partitions[1])
	}
	if report.Totals.Overall.ReprocessMessages != 1700 {
		t.Fatalf("overall: %+v", report.Totals.Overall)
	}
	if !report.WasDryRun {
		t.Fatalf("WasDryRun should be true")
	}
	if report.ResetCommand == "" {
		t.Fatalf("expected reset command to be non-empty")
	}
	if !strings.Contains(report.ResetCommand, "--reset-offsets") {
		t.Fatalf("reset command should mention --reset-offsets: %q", report.ResetCommand)
	}
}

func TestRun_NoOffsetsAndNoTopic(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: nil,
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers: "localhost:9092",
		Group:   "fresh",
		Target:  reset.Target{Kind: reset.KindLatest},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.NewGroup {
		t.Fatalf("expected NewGroup=true; got %+v", report)
	}
	if len(report.Partitions) != 0 {
		t.Fatalf("expected zero partitions, got %d", len(report.Partitions))
	}
}

func TestRun_ActiveMembersAreFlagged(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "orders", Partition: 0, Committed: 100},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 200},
		},
		GroupResult: admin.GroupDescription{
			State: "Stable",
			Members: []admin.MemberInfo{
				{MemberID: "m1", ClientID: "app-1"},
				{MemberID: "m2", ClientID: "app-2"},
			},
		},
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers: "localhost:9092",
		Group:   "payments",
		Target:  reset.Target{Kind: reset.KindEarliest},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.ActiveMembers == nil || report.ActiveMembers.MemberCount != 2 {
		t.Fatalf("expected 2 active members, got %+v", report.ActiveMembers)
	}
	if report.ActiveMembers.State != "Stable" || report.GroupState != "Stable" {
		t.Fatalf("state: %s / %s", report.ActiveMembers.State, report.GroupState)
	}
}

func TestRun_TimeTarget(t *testing.T) {
	when := time.Date(2026, 10, 13, 10, 0, 0, 0, time.FixedZone("BDT", 6*3600))
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "orders", Partition: 0, Committed: 1500},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 10_000},
		},
		TimeResults: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 1450},
		},
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers: "localhost:9092",
		Group:   "payments",
		Topic:   "orders",
		Target:  reset.Target{Kind: reset.KindTime, Value: when},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Partitions) != 1 {
		t.Fatalf("parts: %d", len(report.Partitions))
	}
	r := report.Partitions[0]
	if r.Target != 1450 {
		t.Fatalf("target want 1450 got %d (delta=%d, verdict=%s)", r.Target, r.Delta, r.Verdict)
	}
	if r.Delta != -50 || r.Verdict != reset.VerdictReprocess {
		t.Fatalf("expected REPROCESS -50; got %+v", r)
	}
}

func TestRun_TimeTarget_NoMessageAtOrAfter(t *testing.T) {
	when := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "orders", Partition: 0, Committed: 100},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 10_000},
		},
		TimeResults: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: admin.NoOffset},
		},
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers: "localhost:9092",
		Group:   "payments",
		Topic:   "orders",
		Target:  reset.Target{Kind: reset.KindTime, Value: when},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := report.Partitions[0]
	if r.Verdict != reset.VerdictSkip || r.Target != 10_000 || r.Delta != 9_900 {
		t.Fatalf("want SKIP 9900 to latest; got %+v", r)
	}
}

func TestRun_OffsetForPartitionMatch(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "orders", Partition: 0, Committed: 100},
			{Topic: "orders", Partition: 1, Committed: 200},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 1000},
			{Topic: "orders", Partition: 1, Earliest: 0, Latest: 1000},
		},
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers: "localhost:9092",
		Group:   "payments",
		Target:  offsets(reset.PartitionOffset{Topic: "orders", Partition: 0, Offset: 300}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Partitions) != 1 {
		t.Fatalf("want only orders/0, got %+v", report.Partitions)
	}
	p0 := report.Partitions[0]
	if p0.Partition != 0 || p0.Verdict != reset.VerdictSkip || p0.Delta != 200 || p0.Target != 300 {
		t.Fatalf("p0: %+v", p0)
	}
	if !strings.Contains(report.ResetCommand, "--topic orders:0 --to-offset 300") {
		t.Fatalf("reset command: %q", report.ResetCommand)
	}
}

func TestRun_RequiresArgs(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{"no_brokers", Options{Group: "g", Target: reset.Target{Kind: reset.KindLatest}}},
		{"no_group", Options{Brokers: "x", Target: reset.Target{Kind: reset.KindLatest}}},
		{"no_target", Options{Brokers: "x", Group: "g"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Run(context.Background(), &admin.FakeAdminClient{}, tc.opts)
			if err == nil {
				t.Fatalf("want error, got nil")
			}
		})
	}
}

func TestFakeImplementsReadOnlyInterface(t *testing.T) {
	iface := reflect.TypeOf((*admin.AdminClient)(nil)).Elem()
	allowed := []string{
		"Fetch", "List", "Offset", "Describe", "Sample", "Close",
	}
	banned := []string{
		"Commit", "Alter", "Delete", "Create", "Update", "Set",
	}
	for i := 0; i < iface.NumMethod(); i++ {
		m := iface.Method(i).Name
		allowedFlag := false
		for _, a := range allowed {
			if strings.HasPrefix(m, a) {
				allowedFlag = true
				break
			}
		}
		if allowedFlag {
			continue
		}
		for _, b := range banned {
			if strings.HasPrefix(m, b) {
				t.Fatalf("AdminClient contains a write-like method %q — read-only contract broken", m)
			}
		}
	}
}

func TestRun_NoWriteCalls(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "t", Partition: 0, Committed: 100},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "t", Partition: 0, Earliest: 0, Latest: 1000},
		},
	}
	for _, tgt := range []reset.Target{
		{Kind: reset.KindEarliest},
		{Kind: reset.KindLatest},
		{Kind: reset.KindShift, Shift: -10},
		{Kind: reset.KindTime, Value: time.Unix(0, 0)},
		offsets(reset.PartitionOffset{Topic: "t", Partition: 0, Offset: 1}),
	} {
		if _, err := Run(context.Background(), fake, Options{
			Brokers: "x:9092", Group: "g", Target: tgt, SampleRecords: 10,
		}); err != nil {
			t.Fatal(err)
		}
	}
	calls := fake.Calls()
	want := map[string]bool{
		"DescribeGroup:g":   false,
		"FetchOffsets:g":    false,
		"ListOffsets":       false,
		"SampleRecordSizes": false,
		"OffsetForTimestamp:1970-01-01T00:00:00Z": false,
	}
	for _, c := range calls {
		if _, ok := want[c]; !ok {
			t.Fatalf("unexpected call %q (not a known read)", c)
		}
		want[c] = true
	}
	for w, seen := range want {
		if !seen {
			t.Fatalf("expected read-only call %q; full log: %v", w, calls)
		}
	}
}

func TestRun_BytesEstimate(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "orders", Partition: 0, Committed: 100},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 200},
		},
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers:         "x",
		Group:           "g",
		Target:          reset.Target{Kind: reset.KindEarliest},
		BytesPerMessage: 512,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Totals.Overall.ReprocessBytes != 100*512 {
		t.Fatalf("expected %d bytes approx, got %d", 100*512, report.Totals.Overall.ReprocessBytes)
	}
}

func TestRun_TopicScoping(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "orders", Partition: 0, Committed: 10},
			{Topic: "payments", Partition: 0, Committed: 10},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 100},
		},
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers: "x", Group: "g", Topic: "orders", Target: reset.Target{Kind: reset.KindLatest},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Partitions) != 1 || report.Partitions[0].Topic != "orders" {
		t.Fatalf("want only orders, got %+v", report.Partitions)
	}
	if _, ok := report.Totals.ByTopic["payments"]; ok {
		t.Fatal("payments leaked into totals")
	}
}

func TestRun_UncommittedPartitionsAreNew(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "orders", Partition: 0, Committed: 10},
			{Topic: "orders", Partition: 1, Committed: admin.NoOffset},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 100},
			{Topic: "orders", Partition: 1, Earliest: 0, Latest: 100},
		},
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers: "x", Group: "g", Topic: "orders", Target: reset.Target{Kind: reset.KindEarliest},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.NewGroup {
		t.Fatal("NewGroup should be false: partition 0 has a commit")
	}
	if v := report.Partitions[1].Verdict; v != reset.VerdictNew {
		t.Fatalf("p1 verdict: %s", v)
	}
	if report.Totals.Overall.New != 1 || report.Totals.Overall.ReprocessMessages != 10 {
		t.Fatalf("totals: %+v", report.Totals.Overall)
	}
}

func TestRun_FetchErrorIsFatal(t *testing.T) {
	fake := &admin.FakeAdminClient{FetchErr: errors.New("UNKNOWN_TOPIC_OR_PARTITION")}
	_, err := Run(context.Background(), fake, Options{
		Brokers: "x", Group: "g", Topic: "typo", Target: reset.Target{Kind: reset.KindEarliest},
	})
	if err == nil || !strings.Contains(err.Error(), "UNKNOWN_TOPIC") {
		t.Fatalf("want fetch error, got %v", err)
	}
}

func TestRun_DescribeErrorIsWarning(t *testing.T) {
	fake := &admin.FakeAdminClient{
		GroupErr: errors.New("GROUP_AUTHORIZATION_FAILED"),
		Offsets:  []admin.PartitionProgress{{Topic: "t", Partition: 0, Committed: 5}},
		Ranges:   []admin.PartitionRange{{Topic: "t", Partition: 0, Earliest: 0, Latest: 10}},
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers: "x", Group: "g", Target: reset.Target{Kind: reset.KindEarliest},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Warnings) != 1 || !strings.Contains(report.Warnings[0], "active members") {
		t.Fatalf("warnings: %v", report.Warnings)
	}
}

func TestRun_SampledBytes(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "orders", Partition: 0, Committed: 100}, // REPROCESS [20,100)
			{Topic: "orders", Partition: 1, Committed: 20},  // NO CHANGE
		},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 20, Latest: 200},
			{Topic: "orders", Partition: 1, Earliest: 20, Latest: 200},
		},
		SampleSizes: map[admin.TopicPartition]float64{{Topic: "orders", Partition: 0}: 250},
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers: "x", Group: "g", Target: reset.Target{Kind: reset.KindEarliest}, SampleRecords: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []admin.SampleRange{{Topic: "orders", Partition: 0, From: 20, To: 100}}
	if !reflect.DeepEqual(fake.SampleRanges, want) {
		t.Fatalf("sample ranges: %+v", fake.SampleRanges)
	}
	if got := report.Partitions[0].EstBytes; got != 80*250 {
		t.Fatalf("row bytes: %d", got)
	}
	if got := report.Totals.Overall.ReprocessBytes; got != 80*250 {
		t.Fatalf("total bytes: %d", got)
	}
	if !strings.Contains(report.ByteEstimate, "sampled") {
		t.Fatalf("ByteEstimate: %q", report.ByteEstimate)
	}
}

func TestRun_SamplingFailureIsWarning(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets:   []admin.PartitionProgress{{Topic: "t", Partition: 0, Committed: 50}},
		Ranges:    []admin.PartitionRange{{Topic: "t", Partition: 0, Earliest: 0, Latest: 100}},
		SampleErr: errors.New("TOPIC_AUTHORIZATION_FAILED"),
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers: "x", Group: "g", Target: reset.Target{Kind: reset.KindEarliest}, SampleRecords: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Warnings) != 1 || !strings.Contains(report.Warnings[0], "sampling failed") {
		t.Fatalf("warnings: %v", report.Warnings)
	}
	if report.Totals.Overall.ReprocessMessages != 50 {
		t.Fatalf("totals still expected: %+v", report.Totals.Overall)
	}
}

func TestRun_ToOffsetTopicMismatch(t *testing.T) {
	_, err := Run(context.Background(), &admin.FakeAdminClient{}, Options{
		Brokers: "x", Group: "g", Topic: "a",
		Target: offsets(reset.PartitionOffset{Topic: "b"}),
	})
	if err == nil {
		t.Fatal("want mismatch error")
	}
}

func TestBuildResetCommand(t *testing.T) {
	when := time.Date(2026, 10, 13, 10, 0, 7, 0, time.FixedZone("BDT", 6*3600))
	base := "kafka-consumer-groups.sh --bootstrap-server b:9092 --group g --reset-offsets "
	cases := []struct {
		name string
		opt  Options
		want string
	}{
		{"earliest_all", Options{Target: reset.Target{Kind: reset.KindEarliest}},
			base + "--all-topics --to-earliest --execute"},
		{"latest_topic", Options{Topic: "orders", Target: reset.Target{Kind: reset.KindLatest}},
			base + "--topic orders --to-latest --execute"},
		{"time_utc_with_seconds", Options{Topic: "orders", Target: reset.Target{Kind: reset.KindTime, Value: when}},
			base + "--topic orders --to-datetime 2026-10-13T04:00:07.000Z --execute"},
		{"shift", Options{Target: reset.Target{Kind: reset.KindShift, Shift: -1000}},
			base + "--all-topics --shift-by -1000 --execute"},
		{"offset", Options{Target: offsets(reset.PartitionOffset{Topic: "orders", Partition: 3, Offset: 123})},
			base + "--topic orders:3 --to-offset 123 --execute"},
		{"offsets_multi", Options{Target: offsets(
			reset.PartitionOffset{Topic: "orders", Partition: 0, Offset: 1},
			reset.PartitionOffset{Topic: "orders", Partition: 1, Offset: 2})},
			base + "--from-file offsets.csv --execute"},
		{"offsets_user_file", Options{OffsetsFile: "my.csv", Target: offsets(reset.PartitionOffset{Topic: "orders", Partition: 0, Offset: 1})},
			base + "--from-file my.csv --execute"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.opt.Brokers, c.opt.Group = "b:9092", "g"
			if got := BuildResetCommand(c.opt); got != c.want {
				t.Fatalf("\n got: %s\nwant: %s", got, c.want)
			}
		})
	}

	got := BuildResetCommand(Options{Brokers: "b:9092", Group: "my group", CommandConfig: "/etc/kafka/client.properties",
		Target: reset.Target{Kind: reset.KindEarliest}})
	if !strings.Contains(got, "--command-config /etc/kafka/client.properties --group 'my group'") {
		t.Fatalf("quoting/command-config: %s", got)
	}
}

func offsets(pos ...reset.PartitionOffset) reset.Target {
	return reset.Target{Kind: reset.KindOffset, Offsets: pos}
}

func TestRun_MultipleOffsets(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "orders", Partition: 0, Committed: 100},
			{Topic: "orders", Partition: 1, Committed: 100},
			{Topic: "orders", Partition: 2, Committed: 100},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 1000},
			{Topic: "orders", Partition: 1, Earliest: 0, Latest: 1000},
		},
	}
	report, err := Run(context.Background(), fake, Options{
		Brokers: "x", Group: "g",
		Target: offsets(
			reset.PartitionOffset{Topic: "orders", Partition: 0, Offset: 40},
			reset.PartitionOffset{Topic: "orders", Partition: 1, Offset: 150}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Partitions) != 2 {
		t.Fatalf("want 2 partitions, got %+v", report.Partitions)
	}
	if o := report.Totals.Overall; o.ReprocessMessages != 60 || o.SkipMessages != 50 {
		t.Fatalf("totals: %+v", o)
	}
	if report.ResetFileName != "offsets.csv" || report.ResetFile != "orders,0,40\norders,1,150\n" {
		t.Fatalf("reset file: %q %q", report.ResetFileName, report.ResetFile)
	}
}

func TestRun_DuplicateOffsetsRejected(t *testing.T) {
	_, err := Run(context.Background(), &admin.FakeAdminClient{}, Options{
		Brokers: "x", Group: "g",
		Target: offsets(
			reset.PartitionOffset{Topic: "orders", Partition: 0, Offset: 1},
			reset.PartitionOffset{Topic: "orders", Partition: 0, Offset: 2}),
	})
	if err == nil {
		t.Fatal("want duplicate error")
	}
}
