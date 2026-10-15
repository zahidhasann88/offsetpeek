// Package admin defines the read-only Kafka surface offsetpeek uses.
//
// AdminClient deliberately has no write methods; do not add any. Tests
// inject FakeAdminClient through the same interface.
package admin

import (
	"context"
	"time"
)

// NoOffset marks a missing or unknown offset.
const NoOffset int64 = -1

type PartitionProgress struct {
	Topic     string
	Partition int32
	Committed int64 // NoOffset = never committed
	Metadata  string
	Err       error
}

type PartitionRange struct {
	Topic     string
	Partition int32
	Earliest  int64
	Latest    int64
	Err       error
}

type MemberInfo struct {
	MemberID   string
	ClientID   string
	Host       string
	Partitions []int32
}

type GroupDescription struct {
	State        string
	Protocol     string
	ProtocolType string
	Members      []MemberInfo
	Err          error
}

type AdminClient interface {
	// FetchOffsets returns the group's committed offsets. With topics, every
	// partition of those topics is returned (uncommitted ones as NoOffset).
	FetchOffsets(ctx context.Context, group string, topics ...string) ([]PartitionProgress, error)

	ListOffsets(ctx context.Context, partitions ...TopicPartition) ([]PartitionRange, error)

	// OffsetForTimestamp returns, in Earliest, the first offset with a
	// timestamp >= t for each partition.
	OffsetForTimestamp(ctx context.Context, group string, t time.Time, partitions ...TopicPartition) ([]PartitionRange, error)

	DescribeGroup(ctx context.Context, group string) (GroupDescription, error)

	// SampleRecordSizes reads up to perPartition records from the start of
	// each range, without joining a group or committing, and returns the
	// average record size (key + value + headers) per partition.
	SampleRecordSizes(ctx context.Context, perPartition int, ranges ...SampleRange) (map[TopicPartition]float64, error)

	Close()
}

type TopicPartition struct {
	Topic     string
	Partition int32
}

// SampleRange is the half-open offset range [From, To) of one partition.
type SampleRange struct {
	Topic     string
	Partition int32
	From      int64
	To        int64
}
