package kafkaconsumer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/segmentio/kafka-go"
)

const lagPartitionLimit = 256

type lagClient interface {
	Metadata(context.Context, *kafka.MetadataRequest) (*kafka.MetadataResponse, error)
	OffsetFetch(context.Context, *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error)
	ListOffsets(context.Context, *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error)
}

// LagObserver never joins a group, fetches payloads or commits offsets. The
// independent transport lets it observe a stopped consumer through HTTP.
type LagObserver struct {
	client    lagClient
	group     string
	transport *kafka.Transport
	busy      chan struct{}
}

func NewLagObserver(cfg Config) (*LagObserver, error) {
	rc, err := readerConfig(cfg)
	if err != nil {
		return nil, err
	}
	if err := rc.Validate(); err != nil {
		return nil, err
	}
	transport := &kafka.Transport{TLS: rc.Dialer.TLS.Clone(), SASL: rc.Dialer.SASLMechanism, DialTimeout: 2 * time.Second, IdleTimeout: 30 * time.Second}
	client := &kafka.Client{Addr: kafka.TCP(rc.Brokers...), Transport: transport, Timeout: 2 * time.Second}
	return &LagObserver{client: client, transport: transport, group: rc.GroupID, busy: make(chan struct{}, 1)}, nil
}

func (o *LagObserver) Close() {
	if o.transport != nil {
		o.transport.CloseIdleConnections()
	}
}

// Prometheus uses a single two-second budget, including discovery and offsets.
// Overlapping scrapes fail collection immediately rather than queueing more
// broker work. Failed samples never reuse old gauges or expose broker errors.
func (o *LagObserver) Prometheus(ctx context.Context) string {
	const failed = "projection_workflow_lag_observation_up 0\n"
	select {
	case o.busy <- struct{}{}:
		defer func() { <-o.busy }()
	default:
		return failed
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		return failed
	}
	sample, err := o.collect(ctx)
	if err != nil || ctx.Err() != nil {
		return failed
	}
	result := fmt.Sprintf("projection_workflow_lag_observation_up 1\nprojection_workflow_lag_partitions %d\nprojection_workflow_lag_uncommitted_partitions %d\nprojection_workflow_lag_out_of_range_partitions %d\n", sample.partitions, sample.uncommitted, sample.outOfRange)
	if sample.uncommitted > 0 || sample.outOfRange > 0 {
		return result + "projection_workflow_lag_complete 0\n"
	}
	return result + fmt.Sprintf("projection_workflow_lag_complete 1\nprojection_workflow_lag_offsets %d\nprojection_workflow_lag_max_partition_offsets %d\n", sample.total, sample.maximum)
}

type lagSample struct {
	partitions, uncommitted, outOfRange int
	total, maximum                      int64
}

func (o *LagObserver) collect(ctx context.Context) (lagSample, error) {
	const topic = "order.events.v1"
	var sample lagSample
	metadata, err := o.client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return sample, err
	}
	if metadata == nil || len(metadata.Topics) != 1 || metadata.Topics[0].Name != topic || metadata.Topics[0].Error != nil {
		return sample, errors.New("incomplete workflow topic metadata")
	}
	partitions := metadata.Topics[0].Partitions
	if len(partitions) == 0 || len(partitions) > lagPartitionLimit {
		return sample, errors.New("workflow partition count outside observation bounds")
	}
	ids := make([]int, 0, len(partitions))
	requests := make([]kafka.OffsetRequest, 0, 2*len(partitions))
	expected := make(map[int]bool, len(partitions))
	for _, partition := range partitions {
		if partition.ID < 0 || partition.Error != nil || expected[partition.ID] {
			return sample, errors.New("invalid workflow partition metadata")
		}
		expected[partition.ID] = true
		ids = append(ids, partition.ID)
		requests = append(requests, kafka.FirstOffsetOf(partition.ID), kafka.LastOffsetOf(partition.ID))
	}
	commits, err := o.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: o.group, Topics: map[string][]int{topic: ids}})
	if err != nil {
		return sample, err
	}
	if commits == nil || commits.Error != nil || len(commits.Topics) != 1 || len(commits.Topics[topic]) != len(ids) {
		return sample, errors.New("incomplete workflow group offsets")
	}
	committed := make(map[int]int64, len(ids))
	for _, partition := range commits.Topics[topic] {
		_, duplicate := committed[partition.Partition]
		if !expected[partition.Partition] || duplicate || partition.Error != nil || partition.CommittedOffset < -1 {
			return sample, errors.New("invalid workflow group offset")
		}
		committed[partition.Partition] = partition.CommittedOffset
	}
	// Read group offsets first so ordinary forward progress between calls does
	// not produce a committed offset ahead of an earlier log-end sample.
	ends, err := o.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: requests}, IsolationLevel: kafka.ReadUncommitted})
	if err != nil {
		return sample, err
	}
	if ends == nil || len(ends.Topics) != 1 || len(ends.Topics[topic]) != len(ids) {
		return sample, errors.New("incomplete workflow log offsets")
	}
	seen := make(map[int]bool, len(ids))
	sample.partitions = len(ids)
	for _, partition := range ends.Topics[topic] {
		if !expected[partition.Partition] || seen[partition.Partition] || partition.Error != nil || partition.FirstOffset < 0 || partition.LastOffset < partition.FirstOffset {
			return sample, errors.New("invalid workflow log offsets")
		}
		seen[partition.Partition] = true
		offset := committed[partition.Partition]
		if offset == -1 {
			sample.uncommitted++
			continue
		}
		if offset < partition.FirstOffset || offset > partition.LastOffset {
			sample.outOfRange++
			continue
		}
		lag := partition.LastOffset - offset
		if lag > math.MaxInt64-sample.total {
			return sample, errors.New("workflow lag overflow")
		}
		sample.total += lag
		if lag > sample.maximum {
			sample.maximum = lag
		}
	}
	return sample, nil
}
