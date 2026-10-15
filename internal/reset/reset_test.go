package reset

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/zahidhasann88/offsetpeek/internal/admin"
)

const none = admin.NoOffset

func TestCompute_Kinds(t *testing.T) {
	type tc struct {
		name                        string
		committed, earliest, latest int64
		tgt                         Target
		wantVerdict                 Verdict
		wantDelta                   int64
		wantTarget                  int64
		wantNote                    string
	}
	at := func(p int32, off int64) Target {
		return Target{Kind: KindOffset, Offsets: []PartitionOffset{{Topic: "x", Partition: p, Offset: off}}}
	}
	for _, c := range []tc{
		{name: "earliest_no_change_when_at_earliest", committed: 100, earliest: 100, latest: 200,
			tgt: Target{Kind: KindEarliest}, wantVerdict: VerdictNoChange, wantDelta: 0, wantTarget: 100},
		{name: "earliest_reprocess_when_after", committed: 150, earliest: 100, latest: 200,
			tgt: Target{Kind: KindEarliest}, wantVerdict: VerdictReprocess, wantDelta: -50, wantTarget: 100},
		{name: "latest_skip_when_behind", committed: 150, earliest: 100, latest: 200,
			tgt: Target{Kind: KindLatest}, wantVerdict: VerdictSkip, wantDelta: 50, wantTarget: 200},
		{name: "latest_no_change_when_caught_up", committed: 200, earliest: 100, latest: 200,
			tgt: Target{Kind: KindLatest}, wantVerdict: VerdictNoChange, wantDelta: 0, wantTarget: 200},
		{name: "shift_negative_reprocess_50", committed: 150, earliest: 100, latest: 200,
			tgt: Target{Kind: KindShift, Shift: -50}, wantVerdict: VerdictReprocess, wantDelta: -50, wantTarget: 100},
		{name: "shift_positive_skip_25", committed: 150, earliest: 100, latest: 200,
			tgt: Target{Kind: KindShift, Shift: 25}, wantVerdict: VerdictSkip, wantDelta: 25, wantTarget: 175},
		{name: "shift_zero_no_change", committed: 150, earliest: 100, latest: 200,
			tgt: Target{Kind: KindShift, Shift: 0}, wantVerdict: VerdictNoChange, wantDelta: 0, wantTarget: 150},
		{name: "shift_past_earliest_is_clamped", committed: 150, earliest: 100, latest: 200,
			tgt: Target{Kind: KindShift, Shift: -1000}, wantVerdict: VerdictReprocess, wantDelta: -50, wantTarget: 100,
			wantNote: "before earliest"},
		{name: "shift_past_latest_is_clamped", committed: 150, earliest: 100, latest: 200,
			tgt: Target{Kind: KindShift, Shift: 1000}, wantVerdict: VerdictSkip, wantDelta: 50, wantTarget: 200,
			wantNote: "after latest"},
		{name: "shift_without_commit_unresolved", committed: none, earliest: 100, latest: 200,
			tgt: Target{Kind: KindShift, Shift: -10}, wantVerdict: VerdictUnresolved, wantTarget: none,
			wantNote: "no committed offset"},
		{name: "no_commit_earliest_is_new", committed: none, earliest: 100, latest: 200,
			tgt: Target{Kind: KindEarliest}, wantVerdict: VerdictNew, wantTarget: 100, wantDelta: 0},
		{name: "explicit_offset_match", committed: 150, earliest: 100, latest: 200,
			tgt: at(1, 175), wantVerdict: VerdictSkip, wantDelta: 25, wantTarget: 175},
		{name: "explicit_offset_before_earliest_clamped", committed: 200, earliest: 100, latest: 1000,
			tgt: at(1, 5), wantVerdict: VerdictReprocess, wantDelta: -100, wantTarget: 100, wantNote: "before earliest"},
		{name: "explicit_offset_after_latest_clamped", committed: 100, earliest: 100, latest: 1000,
			tgt: at(1, 9999), wantVerdict: VerdictSkip, wantDelta: 900, wantTarget: 1000, wantNote: "after latest"},
		{name: "explicit_offset_other_partition_unresolved", committed: 150, earliest: 100, latest: 200,
			tgt: at(2, 175), wantVerdict: VerdictUnresolved, wantTarget: none},
		{name: "empty_partition", committed: 0, earliest: 0, latest: 0,
			tgt: Target{Kind: KindEarliest}, wantVerdict: VerdictNoChange, wantDelta: 0, wantTarget: 0},
		{name: "compacted_partition_earliest_moved", committed: 10, earliest: 500, latest: 600,
			tgt: Target{Kind: KindEarliest}, wantVerdict: VerdictSkip, wantDelta: 490, wantTarget: 500,
			wantNote: "below earliest"},
		{name: "unknown_bounds_unresolved", committed: 10, earliest: none, latest: none,
			tgt: Target{Kind: KindLatest}, wantVerdict: VerdictUnresolved, wantTarget: none},
		{name: "time_kind_requires_compute_with_time", committed: 10, earliest: 0, latest: 20,
			tgt: Target{Kind: KindTime}, wantVerdict: VerdictUnresolved, wantTarget: none},
		{name: "unknown_kind_unresolved", committed: 100, earliest: 50, latest: 200,
			tgt: Target{Kind: KindUnknown}, wantVerdict: VerdictUnresolved, wantTarget: none},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := Compute("x", 1, c.committed, c.earliest, c.latest, c.tgt)
			check(t, got, c.wantVerdict, c.wantDelta, c.wantTarget, c.wantNote)
		})
	}
}

func TestComputeWithTime(t *testing.T) {
	cases := []struct {
		name                            string
		committed, earliest, latest, tr int64
		wantVerdict                     Verdict
		wantDelta, wantTarget           int64
		wantNote                        string
	}{
		{"resolve_at_offset_140", 150, 100, 200, 140, VerdictReprocess, -10, 140, ""},
		{"resolve_after_latest_clamped", 150, 100, 200, 9999, VerdictSkip, 50, 200, "after latest"},
		{"resolve_before_earliest_clamped", 150, 100, 200, 50, VerdictReprocess, -50, 100, "before earliest"},
		{"no_record_after_time_resolves_to_latest", 150, 100, 200, none, VerdictSkip, 50, 200, "resolves to latest"},
		{"no_record_and_no_bounds_unresolved", 150, none, none, none, VerdictUnresolved, 0, none, ""},
		{"resolve_match_no_change", 150, 100, 200, 150, VerdictNoChange, 0, 150, ""},
		{"no_commit_is_new", none, 100, 200, 140, VerdictNew, 0, 140, ""},
		{"time_on_empty_partition", 0, 0, 0, 0, VerdictNoChange, 0, 0, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ComputeWithTime("x", 1, c.committed, c.earliest, c.latest, c.tr)
			check(t, got, c.wantVerdict, c.wantDelta, c.wantTarget, c.wantNote)
		})
	}
}

func check(t *testing.T, got PerPartitionResult, v Verdict, delta, target int64, note string) {
	t.Helper()
	if got.Verdict != v {
		t.Fatalf("verdict: want %s got %s (notes=%v)", v, got.Verdict, got.Notes)
	}
	if got.Delta != delta {
		t.Fatalf("delta: want %d got %d", delta, got.Delta)
	}
	if got.Target != target {
		t.Fatalf("target: want %d got %d", target, got.Target)
	}
	if note != "" && !strings.Contains(strings.Join(got.Notes, "|"), note) {
		t.Fatalf("want a note containing %q; got %v", note, got.Notes)
	}
}

func TestSummarise_NoBytes(t *testing.T) {
	rows := []PerPartitionResult{
		{Topic: "t1", Partition: 0, Verdict: VerdictReprocess, Delta: -10},
		{Topic: "t1", Partition: 1, Verdict: VerdictReprocess, Delta: -5},
		{Topic: "t1", Partition: 2, Verdict: VerdictSkip, Delta: 7},
		{Topic: "t2", Partition: 0, Verdict: VerdictNoChange},
		{Topic: "t2", Partition: 1, Verdict: VerdictNew},
		{Topic: "t2", Partition: 2, Verdict: VerdictUnresolved},
	}
	totals := Summarise(rows, 0)
	if totals.Overall.ReprocessMessages != 15 || totals.Overall.SkipMessages != 7 {
		t.Fatalf("unexpected overall: %+v", totals.Overall)
	}
	if totals.Overall.NoChange != 1 || totals.Overall.New != 1 || totals.Overall.Unresolved != 1 {
		t.Fatalf("unexpected counts: %+v", totals.Overall)
	}
	if totals.Overall.ReprocessBytes != 0 || totals.Overall.SkipBytes != 0 {
		t.Fatalf("expected zero bytes with no estimate, got %+v", totals.Overall)
	}
	if totals.ByTopic["t1"].ReprocessMessages != 15 || totals.ByTopic["t1"].SkipMessages != 7 {
		t.Fatalf("t1: %+v", totals.ByTopic["t1"])
	}
	if totals.ByTopic["t2"].ReprocessMessages != 0 || totals.ByTopic["t2"].SkipMessages != 0 {
		t.Fatalf("t2: %+v", totals.ByTopic["t2"])
	}
}

func TestSummarise_Bytes(t *testing.T) {
	rows := []PerPartitionResult{
		{Topic: "t1", Partition: 0, Verdict: VerdictReprocess, Delta: -10},
		{Topic: "t1", Partition: 1, Verdict: VerdictSkip, Delta: 5},
		{Topic: "t1", Partition: 2, Verdict: VerdictReprocess, Delta: -4, EstBytes: 4000}, // sampled wins
	}
	totals := Summarise(rows, 100)
	if totals.Overall.ReprocessMessages != 14 {
		t.Fatalf("overall reprocess: %d", totals.Overall.ReprocessMessages)
	}
	if totals.Overall.ReprocessBytes != 10*100+4000 {
		t.Fatalf("reprocess bytes: want 5000 got %d", totals.Overall.ReprocessBytes)
	}
	if totals.Overall.SkipBytes != 500 {
		t.Fatalf("skip bytes: want 500 got %d", totals.Overall.SkipBytes)
	}
	if totals.ByTopic["t1"].ReprocessBytes != 5000 {
		t.Fatalf("t1 reprocess bytes: want 5000 got %d", totals.ByTopic["t1"].ReprocessBytes)
	}
}

func TestParseOffsetSpec(t *testing.T) {
	cases := map[string]struct {
		want  *PartitionOffset
		error bool
	}{
		"orders:0=1234":         {want: &PartitionOffset{Topic: "orders", Partition: 0, Offset: 1234}},
		"  orders : 0 = 1234  ": {want: &PartitionOffset{Topic: "orders", Partition: 0, Offset: 1234}},
		"orders.v2:3=10":        {want: &PartitionOffset{Topic: "orders.v2", Partition: 3, Offset: 10}},
		"2=99":                  {want: &PartitionOffset{Topic: "", Partition: 2, Offset: 99}},
		"":                      {error: true},
		"empty":                 {error: true},
		"orders:0":              {error: true},
		":0=1":                  {error: true},
		"topic:abc=def":         {error: true},
		"topic:0=abc":           {error: true},
		"topic:-1=5":            {error: true},
		"topic:0=-1":            {error: true}, // negative offsets disallowed
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			got, err := ParseOffsetSpec(in)
			if want.error {
				if err == nil {
					t.Fatalf("want error got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if *got != *want.want {
				t.Fatalf("want %+v got %+v", *want.want, *got)
			}
		})
	}
}

func TestKindString(t *testing.T) {
	cases := map[Kind]string{
		KindTime:     "to-time",
		KindEarliest: "to-earliest",
		KindLatest:   "to-latest",
		KindShift:    "shift-by",
		KindOffset:   "to-offset",
	}
	for k, want := range cases {
		if got := k.String(); got != want {
			t.Fatalf("Kind(%d).String() = %q, want %q", k, got, want)
		}
		b, err := json.Marshal(k)
		if err != nil {
			t.Fatal(err)
		}
		var back Kind
		if err := json.Unmarshal(b, &back); err != nil || back != k {
			t.Fatalf("json roundtrip of %s: got %v (%v)", want, back, err)
		}
	}
}

func TestParseOffsetsCSV(t *testing.T) {
	got, err := ParseOffsetsCSV(strings.NewReader("orders,0,5\n\n orders.v2 , 3 , 7 \n"))
	if err != nil {
		t.Fatal(err)
	}
	want := []PartitionOffset{{Topic: "orders", Partition: 0, Offset: 5}, {Topic: "orders.v2", Partition: 3, Offset: 7}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
	for _, bad := range []string{"", "orders,0", "orders,x,1", ",0,1", "orders,0,-1"} {
		if _, err := ParseOffsetsCSV(strings.NewReader(bad)); err == nil {
			t.Fatalf("want error for %q", bad)
		}
	}
}

func TestValidateOffsets(t *testing.T) {
	ok := []PartitionOffset{{Topic: "a", Partition: 0}, {Topic: "a", Partition: 1}, {Topic: "b", Partition: 0}}
	if err := ValidateOffsets(ok); err != nil {
		t.Fatal(err)
	}
	if err := ValidateOffsets(append(ok, PartitionOffset{Topic: "a", Partition: 1, Offset: 9})); err == nil {
		t.Fatal("want duplicate error")
	}
}

func TestCompute_MultipleOffsets(t *testing.T) {
	tgt := Target{Kind: KindOffset, Offsets: []PartitionOffset{{Topic: "x", Partition: 0, Offset: 5}, {Topic: "x", Partition: 1, Offset: 50}}}
	if r := Compute("x", 0, 10, 0, 100, tgt); r.Verdict != VerdictReprocess || r.Target != 5 {
		t.Fatalf("p0: %+v", r)
	}
	if r := Compute("x", 1, 10, 0, 100, tgt); r.Verdict != VerdictSkip || r.Target != 50 {
		t.Fatalf("p1: %+v", r)
	}
}
