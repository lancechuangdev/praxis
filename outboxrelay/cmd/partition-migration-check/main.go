package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"praxis/outboxrelay/internal/partitionmigration"
)

func main() {
	oldCount := flag.Int("old-partitions", 24, "current topic partition count")
	newCount := flag.Int("new-partitions", 48, "proposed topic partition count")
	records := flag.Int("records", 100000, "number of deterministic synthetic records")
	backlog := flag.Int("backlog", 10000, "records pending on the old mapping at cutover")
	skew := flag.Int("hot-percent", 60, "percentage assigned to the dominant key")
	seed := flag.Int64("seed", 42, "deterministic random seed")
	hotKey := flag.String("hot-key", "BTC-USDT", "dominant ordering key")
	otherKeys := flag.String("other-keys", "ETH-USDT,SOL-USDT,DOGE-USDT,XRP-USDT", "comma-separated non-dominant keys")
	flag.Parse()
	result, err := partitionmigration.Simulate(partitionmigration.Config{OldPartitions: *oldCount, NewPartitions: *newCount, Records: *records, Backlog: *backlog, HotPercent: *skew, Seed: *seed, HotKey: *hotKey, OtherKeys: split(*otherKeys)})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	raw, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(raw))
	if result.RemappedKeys == 0 {
		fmt.Fprintln(os.Stderr, "unsafe rehearsal invalid: no supplied key remaps")
		os.Exit(1)
	}
	if result.UnsafeOvertakes == 0 {
		fmt.Fprintln(os.Stderr, "unsafe rehearsal did not expose cross-mapping overlap; choose a remapped key/backlog")
		os.Exit(1)
	}
	if result.DrainBarrierOvertakes != 0 {
		fmt.Fprintln(os.Stderr, "drain barrier allowed overtaking")
		os.Exit(1)
	}
}

func split(raw string) []string {
	var result []string
	for _, v := range strings.Split(raw, ",") {
		if v = strings.TrimSpace(v); v != "" {
			result = append(result, v)
		}
	}
	return result
}
