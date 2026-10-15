// Package render formats a Report as a table or JSON.
package render

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/zahidhasann88/offsetpeek/internal/reset"
)

type ActiveMembers struct {
	Group       string `json:"group"`
	State       string `json:"state"`
	MemberCount int    `json:"member_count"`
}

type Report struct {
	Group         string                     `json:"group"`
	Brokers       string                     `json:"brokers"`
	GroupState    string                     `json:"group_state,omitempty"`
	Target        reset.Target               `json:"target"`
	ActiveMembers *ActiveMembers             `json:"active_members,omitempty"`
	Warnings      []string                   `json:"warnings,omitempty"`
	Partitions    []reset.PerPartitionResult `json:"partitions"`
	Totals        reset.Totals               `json:"totals"`
	ResetCommand  string                     `json:"reset_command"`
	ResetFileName string                     `json:"reset_file_name,omitempty"`
	ResetFile     string                     `json:"reset_file,omitempty"` // CSV the command's --from-file expects
	WasDryRun     bool                       `json:"was_dry_run"`
	NewGroup      bool                       `json:"new_group"`
	ByteEstimate  string                     `json:"byte_estimate,omitempty"`
}

func JSON(r Report, w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

func Table(r Report, w io.Writer) error {
	fmt.Fprintln(w, "offsetpeek — read-only consumer-group offset reset preview")
	fmt.Fprintln(w, "===========================================================")
	fmt.Fprintf(w, "  brokers : %s\n", r.Brokers)
	fmt.Fprintf(w, "  group   : %s", r.Group)
	if r.GroupState != "" {
		fmt.Fprintf(w, " (state=%s)", r.GroupState)
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  target  : %s\n", describeTarget(r.Target))
	fmt.Fprintln(w)

	if r.ActiveMembers != nil && r.ActiveMembers.MemberCount > 0 {
		fmt.Fprintln(w, "  !!! WARNING !!!")
		fmt.Fprintf(w, "  Group has %d ACTIVE member(s) (state=%s). Resetting offsets on a live group is UNSAFE.\n",
			r.ActiveMembers.MemberCount, r.ActiveMembers.State)
		fmt.Fprintln(w, "  Stop all consumers first; kafka-consumer-groups.sh refuses to reset a non-empty group.")
		fmt.Fprintln(w)
	}

	for _, warn := range r.Warnings {
		fmt.Fprintln(w, "  warning: "+warn)
	}
	if len(r.Warnings) > 0 {
		fmt.Fprintln(w)
	}

	if len(r.Partitions) == 0 {
		fmt.Fprintln(w, "  (no partitions to show)")
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TOPIC\tPARTITION\tCOMMITTED\tEARLIEST\tLATEST\tTARGET\tDELTA\tVERDICT")
	fmt.Fprintln(tw, "-----\t---------\t---------\t--------\t------\t------\t-----\t-------")
	for _, p := range r.Partitions {
		verdict := string(p.Verdict)
		switch p.Verdict {
		case reset.VerdictReprocess, reset.VerdictSkip:
			verdict = fmt.Sprintf("%s %d", p.Verdict, absInt(p.Delta))
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			p.Topic, p.Partition,
			fmtOff(p.Committed),
			fmtOff(p.Earliest),
			fmtOff(p.Latest),
			fmtOff(p.Target),
			fmtDelta(p.Delta, p.Verdict),
			verdict,
		)
	}
	tw.Flush()

	var notes []string
	for _, p := range r.Partitions {
		for _, n := range p.Notes {
			notes = append(notes, fmt.Sprintf("  %s/%d: %s", p.Topic, p.Partition, n))
		}
	}
	if len(notes) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Notes")
		fmt.Fprintln(w, "-----")
		fmt.Fprintln(w, strings.Join(notes, "\n"))
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "Summary by topic")
	fmt.Fprintln(w, "----------------")
	topics := make([]string, 0, len(r.Totals.ByTopic))
	for topic := range r.Totals.ByTopic {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	for _, topic := range topics {
		fmt.Fprintf(w, "  %-32s  %s\n", topic, summaryLine(r.Totals.ByTopic[topic]))
	}

	o := r.Totals.Overall
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Overall")
	fmt.Fprintln(w, "-------")
	fmt.Fprintf(w, "  REPROCESS : %d msgs%s\n", o.ReprocessMessages, bytesSuffix(o.ReprocessBytes))
	fmt.Fprintf(w, "  SKIP      : %d msgs%s\n", o.SkipMessages, bytesSuffix(o.SkipBytes))
	fmt.Fprintf(w, "  NO CHANGE : %d partitions\n", o.NoChange)
	fmt.Fprintf(w, "  NEW       : %d partitions\n", o.New)
	fmt.Fprintf(w, "  UNRESOLVED: %d partitions\n", o.Unresolved)
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  Estimated reprocess volume: %d messages", o.ReprocessMessages)
	if r.ByteEstimate != "" {
		fmt.Fprintf(w, ", ≈ %s (%s)", humanBytes(o.ReprocessBytes), r.ByteEstimate)
	}
	fmt.Fprintln(w)

	if r.ResetCommand != "" {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Equivalent kafka-consumer-groups.sh command (printed only, NOT executed by offsetpeek):")
		fmt.Fprintln(w, "-------------------------------------------------------------------------------------")
		fmt.Fprintln(w, r.ResetCommand)
		if r.ResetFile != "" {
			fmt.Fprintf(w, "\nwith %s:\n%s", r.ResetFileName, r.ResetFile)
		}
	}
	return nil
}

func summaryLine(t reset.TopicTotals) string {
	var ts []string
	if t.ReprocessMessages > 0 {
		ts = append(ts, fmt.Sprintf("REPROCESS %d msgs%s", t.ReprocessMessages, bytesSuffix(t.ReprocessBytes)))
	}
	if t.SkipMessages > 0 {
		ts = append(ts, fmt.Sprintf("SKIP %d msgs%s", t.SkipMessages, bytesSuffix(t.SkipBytes)))
	}
	if t.NoChange > 0 {
		ts = append(ts, fmt.Sprintf("%d no-change", t.NoChange))
	}
	if t.New > 0 {
		ts = append(ts, fmt.Sprintf("%d new (no prior commit)", t.New))
	}
	if t.Unresolved > 0 {
		ts = append(ts, fmt.Sprintf("%d unresolved", t.Unresolved))
	}
	if len(ts) == 0 {
		return "(nothing)"
	}
	return strings.Join(ts, " | ")
}

func bytesSuffix(n int64) string {
	if n <= 0 {
		return ""
	}
	return fmt.Sprintf(" (≈ %s)", humanBytes(n))
}

func describeTarget(t reset.Target) string {
	switch t.Kind {
	case reset.KindTime:
		return fmt.Sprintf("time %s", t.Value.Format(timeRFC3339))
	case reset.KindEarliest:
		return "earliest"
	case reset.KindLatest:
		return "latest"
	case reset.KindShift:
		return fmt.Sprintf("shift by %d messages", t.Shift)
	case reset.KindOffset:
		parts := make([]string, len(t.Offsets))
		for i, po := range t.Offsets {
			parts[i] = fmt.Sprintf("%s:%d=%d", po.Topic, po.Partition, po.Offset)
		}
		return "offset " + strings.Join(parts, ", ")
	}
	return "unknown"
}

const timeRFC3339 = "2006-01-02T15:04:05Z07:00"

func fmtOff(o int64) string {
	if o < 0 {
		return "—"
	}
	return fmt.Sprintf("%d", o)
}

func fmtDelta(d int64, v reset.Verdict) string {
	switch v {
	case reset.VerdictNew:
		return "—"
	case reset.VerdictUnresolved:
		return "?"
	}
	if d > 0 {
		return fmt.Sprintf("+%d", d)
	}
	return fmt.Sprintf("%d", d)
}

func absInt(d int64) int64 {
	if d < 0 {
		return -d
	}
	return d
}

func humanBytes(n int64) string {
	const (
		kb = 1024
		mb = 1024 * 1024
		gb = 1024 * 1024 * 1024
	)
	switch {
	case n >= gb:
		return fmt.Sprintf("%.2f GiB", float64(n)/float64(gb))
	case n >= mb:
		return fmt.Sprintf("%.2f MiB", float64(n)/float64(mb))
	case n >= kb:
		return fmt.Sprintf("%.2f KiB", float64(n)/float64(kb))
	}
	return fmt.Sprintf("%d B", n)
}
