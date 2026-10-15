package admin

import (
	"context"
	"sync"
	"time"
)

// FakeAdminClient is an in-memory AdminClient for tests. CallLog records
// every call so tests can assert that only reads were made.
type FakeAdminClient struct {
	mu sync.Mutex

	Offsets     []PartitionProgress
	Ranges      []PartitionRange
	TimeResults []PartitionRange
	GroupResult GroupDescription
	GroupErr    error
	FetchErr    error

	SampleSizes  map[TopicPartition]float64
	SampleErr    error
	SampleRanges []SampleRange // ranges SampleRecordSizes was asked for

	CallLog []string
}

func (f *FakeAdminClient) record(method string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.CallLog = append(f.CallLog, method)
}

func (f *FakeAdminClient) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.CallLog...)
}

func (f *FakeAdminClient) FetchOffsets(ctx context.Context, group string, topics ...string) ([]PartitionProgress, error) {
	f.record("FetchOffsets:" + group)
	if f.FetchErr != nil {
		return nil, f.FetchErr
	}
	return append([]PartitionProgress(nil), f.Offsets...), nil
}

func (f *FakeAdminClient) ListOffsets(ctx context.Context, partitions ...TopicPartition) ([]PartitionRange, error) {
	f.record("ListOffsets")
	return append([]PartitionRange(nil), f.Ranges...), nil
}

func (f *FakeAdminClient) OffsetForTimestamp(ctx context.Context, group string, t time.Time, partitions ...TopicPartition) ([]PartitionRange, error) {
	f.record("OffsetForTimestamp:" + t.UTC().Format(time.RFC3339))
	return append([]PartitionRange(nil), f.TimeResults...), nil
}

func (f *FakeAdminClient) DescribeGroup(ctx context.Context, group string) (GroupDescription, error) {
	f.record("DescribeGroup:" + group)
	return f.GroupResult, f.GroupErr
}

func (f *FakeAdminClient) SampleRecordSizes(ctx context.Context, perPartition int, ranges ...SampleRange) (map[TopicPartition]float64, error) {
	f.record("SampleRecordSizes")
	f.mu.Lock()
	f.SampleRanges = append(f.SampleRanges, ranges...)
	f.mu.Unlock()
	if f.SampleErr != nil {
		return nil, f.SampleErr
	}
	out := map[TopicPartition]float64{}
	for _, r := range ranges {
		tp := TopicPartition{r.Topic, r.Partition}
		if v, ok := f.SampleSizes[tp]; ok {
			out[tp] = v
		}
	}
	return out, nil
}

func (f *FakeAdminClient) Close() {
	f.record("Close")
}
