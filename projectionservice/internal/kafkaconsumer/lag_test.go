package kafkaconsumer

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeLagClient struct {
	topic        string
	metadata     *kafka.MetadataResponse
	commits      *kafka.OffsetFetchResponse
	ends         *kafka.ListOffsetsResponse
	fail         string
	metadataHook func(context.Context)
}

func (f *fakeLagClient) Metadata(ctx context.Context, req *kafka.MetadataRequest) (*kafka.MetadataResponse, error) {
	if f.metadataHook != nil {
		f.metadataHook(ctx)
	}
	if f.fail == "metadata" {
		return nil, errors.New("private broker error")
	}
	return f.metadata, nil
}
func (f *fakeLagClient) OffsetFetch(_ context.Context, req *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error) {
	if req.GroupID != "test-group" || len(req.Topics) != 1 {
		panic("wrong observation scope")
	}
	if f.fail == "commits" {
		return nil, errors.New("private group error")
	}
	return f.commits, nil
}
func (f *fakeLagClient) ListOffsets(_ context.Context, req *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error) {
	topic := f.topic
	if topic == "" {
		topic = "order.events.v1"
	}
	if len(req.Topics) != 1 || len(req.Topics[topic]) != 4 {
		panic("wrong log offset scope")
	}
	if f.fail == "ends" {
		return nil, errors.New("private log error")
	}
	return f.ends, nil
}

func lagFixture() (*LagObserver, *fakeLagClient) {
	f := &fakeLagClient{
		metadata: &kafka.MetadataResponse{Topics: []kafka.Topic{{Name: "order.events.v1", Partitions: []kafka.Partition{{ID: 0}, {ID: 1}}}}},
		commits:  &kafka.OffsetFetchResponse{Topics: map[string][]kafka.OffsetFetchPartition{"order.events.v1": {{Partition: 0, CommittedOffset: 10}, {Partition: 1, CommittedOffset: 5}}}},
		ends:     &kafka.ListOffsetsResponse{Topics: map[string][]kafka.PartitionOffsets{"order.events.v1": {{Partition: 0, FirstOffset: 0, LastOffset: 20}, {Partition: 1, FirstOffset: 0, LastOffset: 12}}}},
	}
	return &LagObserver{client: f, group: "test-group", busy: make(chan struct{}, 1)}, f
}

func TestLagUsesGroupNextOffsetsAndCompletePartitionCoverage(t *testing.T) {
	o, f := lagFixture()
	f.metadataHook = func(ctx context.Context) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 2*time.Second {
			t.Error("unbounded observation")
		}
	}
	sample := o.Prometheus(context.Background())
	for _, metric := range []string{"projection_workflow_lag_observation_up 1\n", "projection_workflow_lag_complete 1\n", "projection_workflow_lag_partitions 2\n", "projection_workflow_lag_offsets 17\n", "projection_workflow_lag_max_partition_offsets 10\n"} {
		if !strings.Contains(sample, metric) {
			t.Fatal(sample)
		}
	}
	f.commits.Topics["order.events.v1"][0].CommittedOffset = 20
	f.commits.Topics["order.events.v1"][1].CommittedOffset = 12
	if sample := o.Prometheus(context.Background()); !strings.Contains(sample, "projection_workflow_lag_offsets 0\n") {
		t.Fatal(sample)
	}
}

func TestLagUnknownOrOutOfRetentionNeverLooksCaughtUp(t *testing.T) {
	for _, offset := range []int64{-1, 1, 21} {
		o, f := lagFixture()
		f.commits.Topics["order.events.v1"][0].CommittedOffset = offset
		f.ends.Topics["order.events.v1"][0].FirstOffset = 2
		sample := o.Prometheus(context.Background())
		if !strings.Contains(sample, "projection_workflow_lag_observation_up 1\n") || !strings.Contains(sample, "projection_workflow_lag_complete 0\n") || strings.Contains(sample, "projection_workflow_lag_offsets ") {
			t.Fatal(sample)
		}
		metric := "projection_workflow_lag_out_of_range_partitions 1\n"
		if offset == -1 {
			metric = "projection_workflow_lag_uncommitted_partitions 1\n"
		}
		if !strings.Contains(sample, metric) {
			t.Fatal(sample)
		}
	}
}

func TestLagMalformedPartialAndFailedResponsesOmitAllGauges(t *testing.T) {
	for _, mutate := range []func(*fakeLagClient){
		func(f *fakeLagClient) { f.fail = "metadata" }, func(f *fakeLagClient) { f.fail = "commits" }, func(f *fakeLagClient) { f.fail = "ends" },
		func(f *fakeLagClient) { f.metadata = nil }, func(f *fakeLagClient) { f.metadata.Topics[0].Error = kafka.TopicAuthorizationFailed },
		func(f *fakeLagClient) { f.metadata.Topics[0].Partitions[1].ID = 0 },
		func(f *fakeLagClient) { f.metadata.Topics[0].Partitions = make([]kafka.Partition, lagPartitionLimit+1) },
		func(f *fakeLagClient) { f.commits.Topics["order.events.v1"] = f.commits.Topics["order.events.v1"][:1] },
		func(f *fakeLagClient) { f.commits.Topics["order.events.v1"][1].Partition = 0 },
		func(f *fakeLagClient) { f.commits.Topics["order.events.v1"][1].CommittedOffset = -2 },
		func(f *fakeLagClient) { f.commits.Error = kafka.GroupAuthorizationFailed },
		func(f *fakeLagClient) { f.ends.Topics["order.events.v1"][1].Partition = 0 },
		func(f *fakeLagClient) { f.ends.Topics["order.events.v1"][1].FirstOffset = 13 },
		func(f *fakeLagClient) { f.ends.Topics["order.events.v1"][1].Error = kafka.NotLeaderForPartition },
		func(f *fakeLagClient) {
			for i := range 2 {
				f.commits.Topics["order.events.v1"][i].CommittedOffset = 0
				f.ends.Topics["order.events.v1"][i].LastOffset = math.MaxInt64
			}
		},
	} {
		o, f := lagFixture()
		mutate(f)
		if sample := o.Prometheus(context.Background()); sample != "projection_workflow_lag_observation_up 0\n" {
			t.Fatal(sample)
		}
	}
}

func TestLagOverlappingScrapesAreBounded(t *testing.T) {
	o, f := lagFixture()
	entered, release := make(chan struct{}), make(chan struct{})
	f.metadataHook = func(context.Context) { close(entered); <-release }
	done := make(chan string, 1)
	go func() { done <- o.Prometheus(context.Background()) }()
	<-entered
	if sample := o.Prometheus(context.Background()); sample != "projection_workflow_lag_observation_up 0\n" {
		t.Fatal(sample)
	}
	close(release)
	if sample := <-done; !strings.Contains(sample, "projection_workflow_lag_observation_up 1\n") {
		t.Fatal(sample)
	}
}

func TestCancelledLagScrapeDoesNotQueryBroker(t *testing.T) {
	o, f := lagFixture()
	f.metadataHook = func(context.Context) { t.Fatal("cancelled scrape queried broker") }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sample := o.Prometheus(ctx); sample != "projection_workflow_lag_observation_up 0\n" {
		t.Fatal(sample)
	}
}

func TestJournalLagObservesJournalTopic(t *testing.T) {
	o, f := lagFixture()
	o.topic = "ledger.journals.v1"
	f.topic = o.topic
	f.metadata.Topics[0].Name = o.topic
	f.commits.Topics = map[string][]kafka.OffsetFetchPartition{o.topic: f.commits.Topics["order.events.v1"]}
	f.ends.Topics = map[string][]kafka.PartitionOffsets{o.topic: f.ends.Topics["order.events.v1"]}
	sample := o.Prometheus(context.Background())
	if !strings.Contains(sample, "projection_journal_lag_offsets 17\n") || strings.Contains(sample, "projection_workflow_") {
		t.Fatal(sample)
	}
}
