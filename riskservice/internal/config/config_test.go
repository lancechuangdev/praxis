package config

import "testing"

func TestLoadRejectsInvalidPolicy(t *testing.T) {
	t.Setenv("RISK_REJECT_BPS", "10001")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid rejection rate")
	}
}

func TestLoadPolicy(t *testing.T) {
	t.Setenv("RISK_POLICY_VERSION", "policy-7")
	t.Setenv("RISK_REJECT_BPS", "25")
	t.Setenv("RISK_DECISION_LATENCY", "5ms")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.PolicyVersion != "policy-7" || c.RejectBPS != 25 || c.DecisionLatency.String() != "5ms" {
		t.Fatalf("config=%+v", c)
	}
}
