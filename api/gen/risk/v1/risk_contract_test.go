package riskv1

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestCheckOrderContractRoundTripPreservesExactInputs(t *testing.T) {
	want := &CheckOrderRequest{RequestId: "request-1", OrderId: "order-1", UserId: "alice", Symbol: "BTC-USDT", Side: "buy", OrderType: "limit", Quantity: "1000000000000000001", Price: "70000.125", ReserveAssetId: "asset_usdt", ReserveAmountAtomic: "70000125000000", CorrelationId: "correlation-1", CausationId: "causation-1"}
	raw, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got CheckOrderRequest
	if err = proto.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(want, &got) {
		t.Fatalf("round trip changed request: got=%v want=%v", &got, want)
	}
}

func TestRiskDecisionValuesAreStable(t *testing.T) {
	if RiskDecision_RISK_DECISION_UNSPECIFIED != 0 || RiskDecision_RISK_DECISION_APPROVED != 1 || RiskDecision_RISK_DECISION_REJECTED != 2 {
		t.Fatal("risk decision wire values changed")
	}
	response := &CheckOrderResponse{Decision: RiskDecision_RISK_DECISION_REJECTED, DecisionId: "decision-1", ReasonCode: "account_limit", PolicyVersion: "policy-7"}
	raw, err := proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var decoded CheckOrderResponse
	if err = proto.Unmarshal(raw, &decoded); err != nil || !proto.Equal(response, &decoded) {
		t.Fatalf("response round trip=%v err=%v", &decoded, err)
	}
}
