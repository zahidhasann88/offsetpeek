package admin

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KadmClient implements AdminClient with franz-go's kadm (reads only).
type KadmClient struct {
	ac *kadm.Client
	cl *kgo.Client // optional; enables SampleRecordSizes
}

func NewKadmClient(ac *kadm.Client) *KadmClient { return &KadmClient{ac: ac} }

// WithSampling enables SampleRecordSizes, which builds a short-lived
// group-less consumer from cl's options.
func (k *KadmClient) WithSampling(cl *kgo.Client) *KadmClient {
	k.cl = cl
	return k
}

// Close is a no-op; the caller owns and closes the kgo client.
func (k *KadmClient) Close() {}

func (k *KadmClient) FetchOffsets(ctx context.Context, group string, topics ...string) ([]PartitionProgress, error) {
	var (
		res kadm.OffsetResponses
		err error
	)
	if len(topics) == 0 {
		res, err = k.ac.FetchOffsets(ctx, group)
	} else {
		res, err = k.ac.FetchOffsetsForTopics(ctx, group, topics...)
	}
	if errors.Is(err, kerr.GroupIDNotFound) {
		// Never-committed group: report the topics' partitions as NoOffset.
		return k.uncommitted(ctx, topics)
	}
	if err != nil {
		return nil, err
	}
	out := flattenOffsetResponses(res)
	sortProgress(out)
	return out, nil
}

func (k *KadmClient) uncommitted(ctx context.Context, topics []string) ([]PartitionProgress, error) {
	if len(topics) == 0 {
		return nil, nil
	}
	ends, err := k.ac.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, err
	}
	var out []PartitionProgress
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			err = o.Err
			return
		}
		out = append(out, PartitionProgress{Topic: o.Topic, Partition: o.Partition, Committed: NoOffset})
	})
	if err != nil {
		return nil, err
	}
	sortProgress(out)
	return out, nil
}

func (k *KadmClient) ListOffsets(ctx context.Context, partitions ...TopicPartition) ([]PartitionRange, error) {
	if len(partitions) == 0 {
		return nil, nil
	}
	out := make([]PartitionRange, 0, len(partitions))
	for topic, ps := range groupByTopic(partitions) {
		earliest, err := k.ac.ListStartOffsets(ctx, topic)
		if err != nil {
			return nil, err
		}
		latest, err := k.ac.ListEndOffsets(ctx, topic)
		if err != nil {
			return nil, err
		}
		for _, part := range ps {
			r := PartitionRange{Topic: topic, Partition: part, Earliest: NoOffset, Latest: NoOffset}
			if e, ok := earliest.Lookup(topic, part); ok {
				if e.Err != nil {
					r.Err = e.Err
				} else {
					r.Earliest = e.Offset
				}
			}
			if e, ok := latest.Lookup(topic, part); ok {
				if e.Err != nil {
					r.Err = e.Err
				} else {
					r.Latest = e.Offset
				}
			}
			out = append(out, r)
		}
	}
	sortRanges(out)
	return out, nil
}

// OffsetForTimestamp returns the first offset with timestamp >= t, or the
// log end offset if no record is that new (as --to-datetime does).
func (k *KadmClient) OffsetForTimestamp(ctx context.Context, group string, t time.Time, partitions ...TopicPartition) ([]PartitionRange, error) {
	if len(partitions) == 0 {
		return nil, nil
	}
	out := make([]PartitionRange, 0, len(partitions))
	for topic, ps := range groupByTopic(partitions) {
		listed, err := k.ac.ListOffsetsAfterMilli(ctx, t.UnixMilli(), topic)
		if err != nil {
			return nil, err
		}
		for _, part := range ps {
			entry, ok := listed.Lookup(topic, part)
			if !ok {
				out = append(out, PartitionRange{
					Topic: topic, Partition: part, Earliest: NoOffset,
					Err: ErrNotInResponse,
				})
				continue
			}
			off := entry.Offset
			if entry.Err != nil {
				off = NoOffset
			}
			out = append(out, PartitionRange{
				Topic:     topic,
				Partition: part,
				Earliest:  off,
				Err:       entry.Err,
			})
		}
	}
	sortRanges(out)
	return out, nil
}

func (k *KadmClient) DescribeGroup(ctx context.Context, group string) (GroupDescription, error) {
	dres, err := k.ac.DescribeGroups(ctx, group)
	if err != nil {
		return GroupDescription{Err: err}, err
	}
	res, ok := dres[group]
	if !ok {
		return GroupDescription{Err: errGroupNotFound}, errGroupNotFound
	}
	gd := GroupDescription{
		Err:          res.Err,
		State:        res.State,
		Protocol:     res.Protocol,
		ProtocolType: res.ProtocolType,
	}
	for _, m := range res.Members {
		parts := []int32{}
		if c, ok := m.Assigned.AsConsumer(); ok && c != nil {
			for _, t := range c.Topics {
				parts = append(parts, t.Partitions...)
			}
		}
		gd.Members = append(gd.Members, MemberInfo{
			MemberID:   m.MemberID,
			ClientID:   m.ClientID,
			Host:       m.ClientHost,
			Partitions: parts,
		})
	}
	return gd, res.Err
}

// Sampling is best effort: it stops after sampleTimeout, or after sampleIdle
// without a record (e.g. a compacted tail), and uses what it has.
const (
	sampleTimeout = 10 * time.Second
	sampleIdle    = 2 * time.Second
)

// SampleRecordSizes uses a consumer without the ConsumerGroup option, so kgo
// never joins a group or commits.
func (k *KadmClient) SampleRecordSizes(ctx context.Context, perPartition int, ranges ...SampleRange) (map[TopicPartition]float64, error) {
	out := map[TopicPartition]float64{}
	if k.cl == nil {
		return nil, errSamplingDisabled
	}
	start := map[string]map[int32]kgo.Offset{}
	stop := map[TopicPartition]int64{}
	for _, r := range ranges {
		if perPartition <= 0 || r.From < 0 || r.To <= r.From {
			continue
		}
		if start[r.Topic] == nil {
			start[r.Topic] = map[int32]kgo.Offset{}
		}
		start[r.Topic][r.Partition] = kgo.NewOffset().At(r.From)
		stop[TopicPartition{r.Topic, r.Partition}] = min(r.To, r.From+int64(perPartition))
	}
	if len(stop) == 0 {
		return out, nil
	}

	opts := append(k.cl.Opts(), kgo.ConsumePartitions(start))
	cons, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	defer cons.Close()

	ctx, cancel := context.WithTimeout(ctx, sampleTimeout)
	defer cancel()

	type acc struct{ n, bytes int64 }
	accs := map[TopicPartition]*acc{}
	done := map[TopicPartition]bool{}
	var fetchErr error
	finish := func(tp TopicPartition) {
		done[tp] = true
		cons.PauseFetchPartitions(map[string][]int32{tp.Topic: {tp.Partition}})
	}
	for len(done) < len(stop) {
		pollCtx, pollCancel := context.WithTimeout(ctx, sampleIdle)
		fs := cons.PollFetches(pollCtx)
		idle := pollCtx.Err() != nil
		pollCancel()
		if idle {
			break // idle or overall deadline: use what we have
		}
		fs.EachError(func(_ string, _ int32, err error) {
			if fetchErr == nil {
				fetchErr = err
			}
		})
		fs.EachRecord(func(r *kgo.Record) {
			tp := TopicPartition{r.Topic, r.Partition}
			end, ok := stop[tp]
			if !ok || done[tp] {
				return
			}
			// Compacted partitions can jump past the end of the range.
			if r.Offset >= end {
				finish(tp)
				return
			}
			a := accs[tp]
			if a == nil {
				a = &acc{}
				accs[tp] = a
			}
			a.n++
			a.bytes += recordSize(r)
			if r.Offset+1 >= end {
				finish(tp)
			}
		})
		if fetchErr != nil && len(accs) == 0 {
			return nil, fetchErr
		}
	}
	for tp, a := range accs {
		out[tp] = float64(a.bytes) / float64(a.n)
	}
	return out, nil
}

func recordSize(r *kgo.Record) int64 {
	n := len(r.Key) + len(r.Value)
	for _, h := range r.Headers {
		n += len(h.Key) + len(h.Value)
	}
	return int64(n)
}

func flattenOffsetResponses(res kadm.OffsetResponses) []PartitionProgress {
	var out []PartitionProgress
	for topic, partitions := range res {
		for partition, r := range partitions {
			out = append(out, PartitionProgress{
				Topic:     topic,
				Partition: partition,
				Committed: r.At,
				Metadata:  r.Metadata,
				Err:       r.Err,
			})
		}
	}
	return out
}

func sortProgress(p []PartitionProgress) {
	sort.Slice(p, func(i, j int) bool {
		if p[i].Topic != p[j].Topic {
			return p[i].Topic < p[j].Topic
		}
		return p[i].Partition < p[j].Partition
	})
}

func sortRanges(rs []PartitionRange) {
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].Topic != rs[j].Topic {
			return rs[i].Topic < rs[j].Topic
		}
		return rs[i].Partition < rs[j].Partition
	})
}

func groupByTopic(tps []TopicPartition) map[string][]int32 {
	out := map[string][]int32{}
	for _, tp := range tps {
		out[tp.Topic] = append(out[tp.Topic], tp.Partition)
	}
	return out
}

type stringErr string

func (s stringErr) Error() string { return string(s) }

const (
	errGroupNotFound = stringErr("group not found")
	ErrNotInResponse = stringErr("no offset returned for partition")
)

var errSamplingDisabled = errors.New("record sampling is not configured for this client")
