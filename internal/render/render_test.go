package render

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/zahidhasann88/offsetpeek/internal/reset"
)

func sampleReport() Report {
	return Report{
		Brokers: "localhost:9092",
		Group:   "payments",
		Target:  reset.Target{Kind: reset.KindEarliest},
		Partitions: []reset.PerPartitionResult{
			{Topic: "orders", Partition: 0, Committed: 1500, Earliest: 0, Latest: 10000, Target: 0, Delta: -1500, Verdict: reset.VerdictReprocess},
			{Topic: "orders", Partition: 1, Committed: 200, Earliest: 0, Latest: 10000, Target: 0, Delta: -200, Verdict: reset.VerdictReprocess},
		},
		Totals: reset.Summarise([]reset.PerPartitionResult{
			{Topic: "orders", Partition: 0, Verdict: reset.VerdictReprocess, Delta: -1500},
			{Topic: "orders", Partition: 1, Verdict: reset.VerdictReprocess, Delta: -200},
		}, 0),
		ResetCommand: "kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group payments --reset-offsets --all-topics --to-earliest --execute",
		WasDryRun:    true,
	}
}

func TestTable_Basics(t *testing.T) {
	var buf bytes.Buffer
	if err := Table(sampleReport(), &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	wants := []string{
		"offsetpeek",
		"orders",
		"REPROCESS",
		"kafka-consumer-groups.sh",
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Fatalf("missing %q in output:\n%s", w, out)
		}
	}
	if !strings.Contains(out, "REPROCESS 1700 msgs") {
		t.Fatalf("expected 'REPROCESS 1700 msgs' (sum of 1500+200) in output:\n%s", out)
	}
}

func TestTable_ActiveMembersWarn(t *testing.T) {
	r := sampleReport()
	r.ActiveMembers = &ActiveMembers{
		Group: "payments", State: "Stable", MemberCount: 2,
	}
	var buf bytes.Buffer
	if err := Table(r, &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "!!! WARNING !!!") {
		t.Fatalf("expected warning block; got:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "2 ACTIVE member") {
		t.Fatalf("expected 2 ACTIVE members line; got:\n%s", buf.String())
	}
}

func TestTable_NewGroupEmpty(t *testing.T) {
	r := sampleReport()
	r.Partitions = nil
	r.NewGroup = true
	var buf bytes.Buffer
	if err := Table(r, &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "no partitions to show") {
		t.Fatalf("expected empty-partitions placeholder; got:\n%s", buf.String())
	}
}

func TestJSON_RoundTrip(t *testing.T) {
	r := sampleReport()
	var buf bytes.Buffer
	if err := JSON(r, &buf); err != nil {
		t.Fatal(err)
	}
	var back Report
	if err := json.Unmarshal(buf.Bytes(), &back); err != nil {
		t.Fatal(err)
	}
	if back.Group != "payments" {
		t.Fatalf("group roundtrip lost: %s", back.Group)
	}
	if len(back.Partitions) != 2 {
		t.Fatalf("partitions lost in roundtrip: %d", len(back.Partitions))
	}
	if !back.WasDryRun {
		t.Fatal("WasDryRun should remain true")
	}
}

func TestTable_NotesWarningsAndBytes(t *testing.T) {
	r := sampleReport()
	r.Warnings = []string{"byte sampling failed: boom"}
	r.Partitions[0].Notes = []string{"target 5 is before earliest 10; clamped"}
	r.ByteEstimate = "sampled up to 100 records/partition"
	r.Partitions[0].EstBytes = 1500 * 200
	r.Totals = reset.Summarise(r.Partitions, 0)
	var buf bytes.Buffer
	if err := Table(r, &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, w := range []string{
		"warning: byte sampling failed: boom",
		"orders/0: target 5 is before earliest 10; clamped",
		"REPROCESS 1500",
		"Estimated reprocess volume: 1700 messages, ≈ 292.97 KiB (sampled up to 100 records/partition)",
	} {
		if !strings.Contains(out, w) {
			t.Fatalf("missing %q in output:\n%s", w, out)
		}
	}
}

func TestTable_SummaryIsSorted(t *testing.T) {
	r := sampleReport()
	r.Partitions = []reset.PerPartitionResult{
		{Topic: "zeta", Verdict: reset.VerdictNoChange},
		{Topic: "alpha", Verdict: reset.VerdictNoChange},
		{Topic: "mid", Verdict: reset.VerdictNoChange},
	}
	r.Totals = reset.Summarise(r.Partitions, 0)
	var buf bytes.Buffer
	if err := Table(r, &buf); err != nil {
		t.Fatal(err)
	}
	summary := buf.String()[strings.Index(buf.String(), "Summary by topic"):]
	a, m, z := strings.Index(summary, "alpha"), strings.Index(summary, "mid"), strings.Index(summary, "zeta")
	if !(a < m && m < z) {
		t.Fatalf("summary not sorted:\n%s", summary)
	}
}
