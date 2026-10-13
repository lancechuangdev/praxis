package partitionmigration

import (
	"strconv"
	"testing"
)

func TestDominantMarketMigrationScenarios(t *testing.T) {
	for _, skew := range []int{60, 80, 95} {
		t.Run(strconv.Itoa(skew), func(t *testing.T) {
			result, err := Simulate(Config{OldPartitions: 24, NewPartitions: 48, Records: 10000, Backlog: 2000, HotPercent: skew, Seed: 42, HotKey: "BTC-USDT", OtherKeys: []string{"ETH-USDT", "SOL-USDT", "DOGE-USDT", "XRP-USDT"}})
			if err != nil {
				t.Fatal(err)
			}
			if result.HotRecords < 5500 || result.UnsafeOvertakes == 0 || result.DrainBarrierOvertakes != 0 {
				t.Fatalf("unexpected %d%% result: %+v", skew, result)
			}
			if result.OldHotPartition == result.NewHotPartition {
				t.Fatalf("fixture must exercise a remapped dominant key: %+v", result)
			}
		})
	}
}

func TestRejectsInvalidScenario(t *testing.T) {
	_, err := Simulate(Config{OldPartitions: 24, NewPartitions: 24, Records: 10, Backlog: 1, HotPercent: 60, HotKey: "BTC-USDT", OtherKeys: []string{"ETH-USDT"}})
	if err == nil {
		t.Fatal("expected invalid partition counts")
	}
}
