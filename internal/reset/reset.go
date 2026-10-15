// Package reset computes per-partition reset targets, deltas and verdicts.
// It is pure (no Kafka I/O) and mirrors kafka-consumer-groups.sh semantics:
// targets are clamped to [earliest, latest], --to-datetime with no newer
// record resolves to latest, and --shift-by needs a committed offset.
package reset

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/zahidhasann88/offsetpeek/internal/admin"
)

type Kind int

const (
	KindUnknown Kind = iota
	KindTime
	KindEarliest
	KindLatest
	KindShift
	KindOffset
)

func (k Kind) String() string {
	switch k {
	case KindTime:
		return "to-time"
	case KindEarliest:
		return "to-earliest"
	case KindLatest:
		return "to-latest"
	case KindShift:
		return "shift-by"
	case KindOffset:
		return "to-offset"
	default:
		return "unknown"
	}
}

func (k Kind) MarshalText() ([]byte, error) { return []byte(k.String()), nil }

func (k *Kind) UnmarshalText(b []byte) error {
	for c := KindTime; c <= KindOffset; c++ {
		if c.String() == string(b) {
			*k = c
			return nil
		}
	}
	*k = KindUnknown
	return nil
}

type Target struct {
	Kind    Kind              `json:"kind"`
	Value   time.Time         `json:"time,omitzero"`
	Shift   int64             `json:"shift,omitempty"`
	Offsets []PartitionOffset `json:"offsets,omitempty"`
}

type PartitionOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// PerPartitionResult is one output row. Offsets of -1 mean none/unknown.
type PerPartitionResult struct {
	Topic          string   `json:"topic"`
	Partition      int32    `json:"partition"`
	Committed      int64    `json:"committed"`
	Earliest       int64    `json:"earliest"`
	Latest         int64    `json:"latest"`
	Target         int64    `json:"target"`
	Delta          int64    `json:"delta"` // target - committed
	Verdict        Verdict  `json:"verdict"`
	Notes          []string `json:"notes,omitempty"`
	AvgRecordBytes float64  `json:"avg_record_bytes,omitempty"`
	EstBytes       int64    `json:"est_bytes,omitempty"`
}

type Verdict string

const (
	VerdictReprocess  Verdict = "REPROCESS"
	VerdictSkip       Verdict = "SKIP"
	VerdictNoChange   Verdict = "NO CHANGE"
	VerdictNew        Verdict = "NEW"
	VerdictUnresolved Verdict = "UNRESOLVED"
)

// Compute resolves a non-time target for one partition. Time targets go
// through ComputeWithTime.
func Compute(topic string, partition int32, committed, earliest, latest int64, tgt Target) PerPartitionResult {
	r := PerPartitionResult{
		Topic:     topic,
		Partition: partition,
		Committed: committed,
		Earliest:  earliest,
		Latest:    latest,
		Target:    admin.NoOffset,
	}

	switch tgt.Kind {
	case KindEarliest:
		if earliest < 0 {
			return unresolved(r, "earliest offset unknown")
		}
		r.Target = earliest
	case KindLatest:
		if latest < 0 {
			return unresolved(r, "latest offset unknown")
		}
		r.Target = latest
	case KindShift:
		if committed == admin.NoOffset {
			return unresolved(r, "no committed offset to shift from; kafka-consumer-groups.sh refuses this partition")
		}
		r.Target = committed + tgt.Shift
	case KindOffset:
		po, ok := tgt.OffsetFor(topic, partition)
		if !ok {
			return unresolved(r, "no explicit target for this partition")
		}
		r.Target = po
	case KindTime:
		return unresolved(r, "use ComputeWithTime for time targets")
	default:
		return unresolved(r, "unknown target kind")
	}
	return finish(r)
}

// OffsetFor returns the explicit offset for a partition of a KindOffset target.
func (t Target) OffsetFor(topic string, partition int32) (int64, bool) {
	for _, po := range t.Offsets {
		if po.Topic == topic && po.Partition == partition {
			return po.Offset, true
		}
	}
	return 0, false
}

// ComputeWithTime takes the offset the broker resolved for the timestamp;
// NoOffset falls back to latest, as kafka-consumer-groups.sh does.
func ComputeWithTime(topic string, partition int32, committed, earliest, latest, timeResolvedOffset int64) PerPartitionResult {
	r := PerPartitionResult{
		Topic:     topic,
		Partition: partition,
		Committed: committed,
		Earliest:  earliest,
		Latest:    latest,
		Target:    timeResolvedOffset,
	}
	if timeResolvedOffset == admin.NoOffset {
		if latest < 0 {
			return unresolved(r, "no offset for the timestamp and latest offset unknown")
		}
		r.Target = latest
		r.Notes = append(r.Notes, "no record at or after the timestamp; resolves to latest")
	}
	return finish(r)
}

func finish(r PerPartitionResult) PerPartitionResult {
	if r.Earliest >= 0 && r.Target < r.Earliest {
		r.Notes = append(r.Notes, fmt.Sprintf("target %d is before earliest %d; clamped", r.Target, r.Earliest))
		r.Target = r.Earliest
	}
	if r.Latest >= 0 && r.Target > r.Latest {
		r.Notes = append(r.Notes, fmt.Sprintf("target %d is after latest %d; clamped", r.Target, r.Latest))
		r.Target = r.Latest
	}

	if r.Committed == admin.NoOffset {
		r.Verdict = VerdictNew
		r.Notes = append(r.Notes, "no committed offset; the reset would create one")
		return r
	}
	if r.Earliest >= 0 && r.Committed < r.Earliest {
		r.Notes = append(r.Notes, fmt.Sprintf("committed %d is below earliest %d (already deleted by retention)", r.Committed, r.Earliest))
	}
	if r.Latest >= 0 && r.Committed > r.Latest {
		r.Notes = append(r.Notes, fmt.Sprintf("committed %d is above latest %d (log truncated?)", r.Committed, r.Latest))
	}

	r.Delta = r.Target - r.Committed
	switch {
	case r.Delta == 0:
		r.Verdict = VerdictNoChange
	case r.Delta < 0:
		r.Verdict = VerdictReprocess
	default:
		r.Verdict = VerdictSkip
	}
	return r
}

func unresolved(r PerPartitionResult, note string) PerPartitionResult {
	r.Verdict = VerdictUnresolved
	r.Notes = append(r.Notes, note)
	return r
}

type Totals struct {
	ByTopic map[string]TopicTotals `json:"by_topic"`
	Overall TopicTotals            `json:"overall"`
}

// TopicTotals counts messages for REPROCESS/SKIP and partitions otherwise.
type TopicTotals struct {
	ReprocessMessages int64 `json:"reprocess_messages"`
	SkipMessages      int64 `json:"skip_messages"`
	NoChange          int64 `json:"no_change_partitions"`
	New               int64 `json:"new_partitions"`
	Unresolved        int64 `json:"unresolved_partitions"`
	ReprocessBytes    int64 `json:"reprocess_bytes,omitempty"`
	SkipBytes         int64 `json:"skip_bytes,omitempty"`
}

func (t *TopicTotals) add(r PerPartitionResult, bytes int64) {
	switch r.Verdict {
	case VerdictReprocess:
		t.ReprocessMessages += -r.Delta
		t.ReprocessBytes += bytes
	case VerdictSkip:
		t.SkipMessages += r.Delta
		t.SkipBytes += bytes
	case VerdictNoChange:
		t.NoChange++
	case VerdictNew:
		t.New++
	case VerdictUnresolved:
		t.Unresolved++
	}
}

// Summarise totals the rows. Bytes come from each row's EstBytes when set,
// else |Delta| * bytesPerMessage (0 disables that fallback).
func Summarise(rows []PerPartitionResult, bytesPerMessage int64) Totals {
	t := Totals{ByTopic: map[string]TopicTotals{}}
	for _, r := range rows {
		bytes := r.EstBytes
		if bytes == 0 && bytesPerMessage > 0 {
			bytes = absDelta(r.Delta) * bytesPerMessage
		}
		tt := t.ByTopic[r.Topic]
		tt.add(r, bytes)
		t.ByTopic[r.Topic] = tt
		t.Overall.add(r, bytes)
	}
	return t
}

func absDelta(d int64) int64 {
	if d < 0 {
		return -d
	}
	return d
}

// ParseOffsetSpec parses "topic:partition=offset", or "partition=offset"
// with Topic left empty for the caller to fill from --topic.
func ParseOffsetSpec(s string) (*PartitionOffset, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, errors.New("empty offset spec")
	}
	eq := strings.LastIndex(s, "=")
	if eq < 0 {
		return nil, fmt.Errorf("offset spec %q: expected topic:partition=offset", s)
	}
	head := s[:eq]
	var topic, pStr string
	if colon := strings.LastIndex(head, ":"); colon >= 0 {
		topic, pStr = strings.TrimSpace(head[:colon]), head[colon+1:]
		if topic == "" {
			return nil, fmt.Errorf("offset spec %q: empty topic", s)
		}
	} else {
		pStr = head
	}
	return parsePartitionOffset(s, topic, pStr, s[eq+1:])
}

// ParseOffsetsCSV reads kafka-consumer-groups.sh --from-file format:
// one "topic,partition,offset" per line.
func ParseOffsetsCSV(r io.Reader) ([]PartitionOffset, error) {
	var out []PartitionOffset
	sc := bufio.NewScanner(r)
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		f := strings.Split(text, ",")
		if len(f) != 3 || strings.TrimSpace(f[0]) == "" {
			return nil, fmt.Errorf("line %d: want topic,partition,offset, got %q", line, text)
		}
		po, err := parsePartitionOffset(fmt.Sprintf("line %d", line), strings.TrimSpace(f[0]), f[1], f[2])
		if err != nil {
			return nil, err
		}
		out = append(out, *po)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, errors.New("offsets file is empty")
	}
	return out, nil
}

// ValidateOffsets rejects duplicate partitions.
func ValidateOffsets(pos []PartitionOffset) error {
	seen := map[admin.TopicPartition]bool{}
	for _, po := range pos {
		tp := admin.TopicPartition{Topic: po.Topic, Partition: po.Partition}
		if seen[tp] {
			return fmt.Errorf("offset for %s:%d given more than once", po.Topic, po.Partition)
		}
		seen[tp] = true
	}
	return nil
}

func parsePartitionOffset(ctx, topic, pStr, oStr string) (*PartitionOffset, error) {
	pStr, oStr = strings.TrimSpace(pStr), strings.TrimSpace(oStr)
	p, err := strconv.ParseInt(pStr, 10, 32)
	if err != nil || p < 0 {
		return nil, fmt.Errorf("%s: partition %q not a non-negative int", ctx, pStr)
	}
	o, err := strconv.ParseInt(oStr, 10, 64)
	if err != nil || o < 0 {
		return nil, fmt.Errorf("%s: offset %q not a non-negative int", ctx, oStr)
	}
	return &PartitionOffset{Topic: topic, Partition: int32(p), Offset: o}, nil
}
