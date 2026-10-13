package partitionmigration

import (
	"fmt"
	"math/rand"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	OldPartitions int
	NewPartitions int
	Records       int
	Backlog       int
	HotPercent    int
	Seed          int64
	HotKey        string
	OtherKeys     []string
}

type Result struct {
	HotKey                string `json:"hot_key"`
	OldHotPartition       int    `json:"old_hot_partition"`
	NewHotPartition       int    `json:"new_hot_partition"`
	HotRecords            int    `json:"hot_records"`
	RemappedKeys          int    `json:"remapped_keys"`
	OldBacklogAtCutover   int    `json:"old_backlog_at_cutover"`
	UnsafeOvertakes       int    `json:"unsafe_overtakes"`
	DrainBarrierOvertakes int    `json:"drain_barrier_overtakes"`
}

func Simulate(c Config) (Result, error) {
	if c.OldPartitions < 1 || c.NewPartitions < 1 || c.OldPartitions == c.NewPartitions {
		return Result{}, fmt.Errorf("old and new partition counts must be positive and different")
	}
	if c.Records < 2 || c.Backlog < 1 || c.Backlog >= c.Records {
		return Result{}, fmt.Errorf("records must be at least 2 and backlog must be between 1 and records-1")
	}
	if c.HotPercent < 1 || c.HotPercent > 100 || c.HotKey == "" || len(c.OtherKeys) == 0 {
		return Result{}, fmt.Errorf("hot percent must be 1..100 and hot/other keys are required")
	}
	oldPartitions := partitionIDs(c.OldPartitions)
	newPartitions := partitionIDs(c.NewPartitions)
	balancer := &kafka.Hash{}
	partition := func(key string, ids []int) int { return balancer.Balance(kafka.Message{Key: []byte(key)}, ids...) }
	result := Result{HotKey: c.HotKey, OldHotPartition: partition(c.HotKey, oldPartitions), NewHotPartition: partition(c.HotKey, newPartitions), OldBacklogAtCutover: c.Backlog}
	seen := map[string]struct{}{c.HotKey: {}}
	for _, key := range c.OtherKeys {
		seen[key] = struct{}{}
	}
	for key := range seen {
		if partition(key, oldPartitions) != partition(key, newPartitions) {
			result.RemappedKeys++
		}
	}

	rng := rand.New(rand.NewSource(c.Seed))
	oldPending := make(map[string]int)
	newRecords := make([]string, 0, c.Records-c.Backlog)
	for i := 0; i < c.Records; i++ {
		key := c.OtherKeys[rng.Intn(len(c.OtherKeys))]
		if rng.Intn(100) < c.HotPercent {
			key = c.HotKey
			result.HotRecords++
		}
		if i < c.Backlog {
			oldPending[key]++
			continue
		}
		newRecords = append(newRecords, key)
	}
	result.UnsafeOvertakes = countOvertakes(oldPending, newRecords, func(key string) bool {
		return partition(key, oldPartitions) != partition(key, newPartitions)
	})
	// A stop-and-drain barrier consumes every old record before admitting a
	// record with the new mapping. Run the same overlap detector after draining.
	drained := make(map[string]int, len(oldPending))
	result.DrainBarrierOvertakes = countOvertakes(drained, newRecords, func(key string) bool {
		return partition(key, oldPartitions) != partition(key, newPartitions)
	})
	return result, nil
}

func countOvertakes(oldPending map[string]int, newRecords []string, remapped func(string) bool) int {
	count := 0
	for _, key := range newRecords {
		if oldPending[key] > 0 && remapped(key) {
			count++
		}
	}
	return count
}

func partitionIDs(count int) []int {
	ids := make([]int, count)
	for i := range ids {
		ids[i] = i
	}
	return ids
}
