// Package preview runs a read-only reset preview against an AdminClient and
// builds the report.
package preview

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/zahidhasann88/offsetpeek/internal/admin"
	"github.com/zahidhasann88/offsetpeek/internal/render"
	"github.com/zahidhasann88/offsetpeek/internal/reset"
)

// DefaultOffsetsFile is the --from-file name used in the printed command
// when several --to-offset targets were given on the command line.
const DefaultOffsetsFile = "offsets.csv"

type Options struct {
	Brokers         string
	Group           string
	Topic           string // optional; empty = every topic the group has committed to
	Target          reset.Target
	BytesPerMessage int64
	SampleRecords   int
	CommandConfig   string // --command-config for the printed command
	OffsetsFile     string // --from-file the user passed, if any
}

func Run(ctx context.Context, a admin.AdminClient, opt Options) (render.Report, error) {
	if opt.Brokers == "" {
		return render.Report{}, errors.New("--brokers (or BROKERS env) is required")
	}
	if opt.Group == "" {
		return render.Report{}, errors.New("--group is required")
	}
	if opt.Target.Kind == reset.KindUnknown {
		return render.Report{}, errors.New("a reset target is required (--to-time / --to-earliest / --to-latest / --shift-by / --to-offset / --from-file)")
	}
	if opt.Target.Kind == reset.KindOffset {
		if len(opt.Target.Offsets) == 0 {
			return render.Report{}, errors.New("--to-offset needs at least one topic:partition=offset")
		}
		for _, po := range opt.Target.Offsets {
			if po.Topic == "" {
				return render.Report{}, errors.New("--to-offset needs a topic (topic:partition=offset, or --topic)")
			}
			if opt.Topic != "" && opt.Topic != po.Topic {
				return render.Report{}, fmt.Errorf("--to-offset topic %q does not match --topic %q", po.Topic, opt.Topic)
			}
		}
		if err := reset.ValidateOffsets(opt.Target.Offsets); err != nil {
			return render.Report{}, err
		}
	}

	report := render.Report{
		Brokers:      opt.Brokers,
		Group:        opt.Group,
		Target:       opt.Target,
		WasDryRun:    true,
		ResetCommand: BuildResetCommand(opt),
	}
	if opt.Target.Kind == reset.KindOffset && len(opt.Target.Offsets) > 1 && opt.OffsetsFile == "" {
		report.ResetFileName = DefaultOffsetsFile
		report.ResetFile = OffsetsCSV(opt.Target.Offsets)
	}

	// Describe first so active members are surfaced before anything else.
	gdesc, err := a.DescribeGroup(ctx, opt.Group)
	if err != nil {
		report.Warnings = append(report.Warnings, fmt.Sprintf("could not describe group (cannot check for active members): %v", err))
	}
	report.GroupState = gdesc.State
	if len(gdesc.Members) > 0 {
		report.ActiveMembers = &render.ActiveMembers{
			Group:       opt.Group,
			State:       gdesc.State,
			MemberCount: len(gdesc.Members),
		}
	}

	committed, err := a.FetchOffsets(ctx, opt.Group, scopeTopics(opt)...)
	if err != nil {
		return report, fmt.Errorf("fetch committed offsets: %w", err)
	}

	tps := collectPartitions(opt, committed)
	report.NewGroup = !anyCommitted(committed, tps)
	if len(tps) == 0 {
		report.Totals = reset.Summarise(nil, opt.BytesPerMessage)
		return report, nil
	}

	bounds, err := a.ListOffsets(ctx, tps...)
	if err != nil {
		return report, fmt.Errorf("list offsets: %w", err)
	}
	boundIndex := map[admin.TopicPartition]admin.PartitionRange{}
	for _, b := range bounds {
		boundIndex[admin.TopicPartition{Topic: b.Topic, Partition: b.Partition}] = b
	}

	timeIndex := map[admin.TopicPartition]int64{}
	if opt.Target.Kind == reset.KindTime {
		out, err := a.OffsetForTimestamp(ctx, opt.Group, opt.Target.Value, tps...)
		if err != nil {
			return report, fmt.Errorf("offset-for-timestamp(%s): %w", opt.Target.Value.Format(time.RFC3339), err)
		}
		for _, r := range out {
			timeIndex[admin.TopicPartition{Topic: r.Topic, Partition: r.Partition}] = r.Earliest
		}
	}

	results := make([]reset.PerPartitionResult, 0, len(tps))
	for _, tp := range tps {
		committedOffset := lookupCommitted(committed, tp)
		earliest, latest := admin.NoOffset, admin.NoOffset
		bound, ok := boundIndex[tp]
		if ok {
			earliest, latest = bound.Earliest, bound.Latest
		}
		var row reset.PerPartitionResult
		if opt.Target.Kind == reset.KindTime {
			resolved, ok := timeIndex[tp]
			if !ok {
				resolved = admin.NoOffset
			}
			row = reset.ComputeWithTime(tp.Topic, tp.Partition, committedOffset, earliest, latest, resolved)
		} else {
			row = reset.Compute(tp.Topic, tp.Partition, committedOffset, earliest, latest, opt.Target)
		}
		if ok && bound.Err != nil {
			row.Notes = append(row.Notes, fmt.Sprintf("listing offsets failed: %v", bound.Err))
		}
		results = append(results, row)
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Topic != results[j].Topic {
			return results[i].Topic < results[j].Topic
		}
		return results[i].Partition < results[j].Partition
	})

	if opt.SampleRecords > 0 {
		if err := sampleBytes(ctx, a, opt.SampleRecords, results); err != nil {
			report.Warnings = append(report.Warnings, fmt.Sprintf("byte sampling failed: %v", err))
		} else {
			report.ByteEstimate = fmt.Sprintf("sampled up to %d records/partition", opt.SampleRecords)
		}
	}
	if report.ByteEstimate == "" && opt.BytesPerMessage > 0 {
		report.ByteEstimate = fmt.Sprintf("assuming %d B/message", opt.BytesPerMessage)
	}

	report.Partitions = results
	report.Totals = reset.Summarise(results, opt.BytesPerMessage)
	return report, nil
}

// sampleBytes fills AvgRecordBytes/EstBytes for rows that move, sampling
// the part of the moved range that still holds records.
func sampleBytes(ctx context.Context, a admin.AdminClient, n int, rows []reset.PerPartitionResult) error {
	var ranges []admin.SampleRange
	for _, r := range rows {
		lo, hi := r.Target, r.Committed
		switch r.Verdict {
		case reset.VerdictReprocess:
		case reset.VerdictSkip:
			lo, hi = r.Committed, r.Target
		default:
			continue
		}
		if r.Earliest >= 0 && lo < r.Earliest {
			lo = r.Earliest
		}
		if r.Latest >= 0 && hi > r.Latest {
			hi = r.Latest
		}
		if lo >= hi {
			continue
		}
		ranges = append(ranges, admin.SampleRange{Topic: r.Topic, Partition: r.Partition, From: lo, To: hi})
	}
	if len(ranges) == 0 {
		return nil
	}
	avgs, err := a.SampleRecordSizes(ctx, n, ranges...)
	if err != nil {
		return err
	}
	for i := range rows {
		avg, ok := avgs[admin.TopicPartition{Topic: rows[i].Topic, Partition: rows[i].Partition}]
		if !ok {
			continue
		}
		rows[i].AvgRecordBytes = avg
		rows[i].EstBytes = int64(math.Round(avg * math.Abs(float64(rows[i].Delta))))
	}
	return nil
}

func scopeTopics(opt Options) []string {
	if opt.Target.Kind == reset.KindOffset {
		seen := map[string]bool{}
		var out []string
		for _, po := range opt.Target.Offsets {
			if !seen[po.Topic] {
				seen[po.Topic] = true
				out = append(out, po.Topic)
			}
		}
		return out
	}
	if opt.Topic != "" {
		return []string{opt.Topic}
	}
	return nil
}

// collectPartitions returns the partitions the reset would touch: the
// explicit ones for --to-offset, else what FetchOffsets returned (limited
// to --topic when set).
func collectPartitions(opt Options, committed []admin.PartitionProgress) []admin.TopicPartition {
	if opt.Target.Kind == reset.KindOffset {
		out := make([]admin.TopicPartition, 0, len(opt.Target.Offsets))
		for _, po := range opt.Target.Offsets {
			out = append(out, admin.TopicPartition{Topic: po.Topic, Partition: po.Partition})
		}
		return out
	}
	seen := map[admin.TopicPartition]bool{}
	var out []admin.TopicPartition
	for _, p := range committed {
		if p.Err != nil || (opt.Topic != "" && p.Topic != opt.Topic) {
			continue
		}
		tp := admin.TopicPartition{Topic: p.Topic, Partition: p.Partition}
		if !seen[tp] {
			seen[tp] = true
			out = append(out, tp)
		}
	}
	return out
}

func anyCommitted(cs []admin.PartitionProgress, tps []admin.TopicPartition) bool {
	for _, tp := range tps {
		if lookupCommitted(cs, tp) != admin.NoOffset {
			return true
		}
	}
	return false
}

func lookupCommitted(cs []admin.PartitionProgress, tp admin.TopicPartition) int64 {
	for _, c := range cs {
		if c.Topic == tp.Topic && c.Partition == tp.Partition && c.Err == nil {
			return c.Committed
		}
	}
	return admin.NoOffset
}

// OffsetsCSV renders offsets in kafka-consumer-groups.sh --from-file format.
func OffsetsCSV(pos []reset.PartitionOffset) string {
	var sb strings.Builder
	for _, po := range pos {
		fmt.Fprintf(&sb, "%s,%d,%d\n", po.Topic, po.Partition, po.Offset)
	}
	return sb.String()
}

// BuildResetCommand returns the kafka-consumer-groups.sh command that
// performs the previewed reset. It is printed only, never executed.
func BuildResetCommand(opt Options) string {
	args := []string{
		"kafka-consumer-groups.sh",
		"--bootstrap-server", opt.Brokers,
	}
	if opt.CommandConfig != "" {
		args = append(args, "--command-config", opt.CommandConfig)
	}
	args = append(args, "--group", opt.Group, "--reset-offsets")

	offsets := opt.Target.Offsets
	switch {
	case opt.Target.Kind == reset.KindOffset && (opt.OffsetsFile != "" || len(offsets) > 1):
		file := opt.OffsetsFile
		if file == "" {
			file = DefaultOffsetsFile
		}
		args = append(args, "--from-file", file)
	case opt.Target.Kind == reset.KindOffset && len(offsets) == 1:
		args = append(args, "--topic", fmt.Sprintf("%s:%d", offsets[0].Topic, offsets[0].Partition),
			"--to-offset", fmt.Sprintf("%d", offsets[0].Offset))
	default:
		if opt.Topic != "" {
			args = append(args, "--topic", opt.Topic)
		} else {
			args = append(args, "--all-topics")
		}
		switch opt.Target.Kind {
		case reset.KindTime:
			args = append(args, "--to-datetime", opt.Target.Value.UTC().Format("2006-01-02T15:04:05.000Z"))
		case reset.KindEarliest:
			args = append(args, "--to-earliest")
		case reset.KindLatest:
			args = append(args, "--to-latest")
		case reset.KindShift:
			args = append(args, "--shift-by", fmt.Sprintf("%d", opt.Target.Shift))
		}
	}
	args = append(args, "--execute")

	for i, a := range args {
		args[i] = shellQuote(a)
	}
	return strings.Join(args, " ")
}

var shellSafe = regexp.MustCompile(`^[A-Za-z0-9._:,=/@+-]+$`)

func shellQuote(s string) string {
	if shellSafe.MatchString(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
