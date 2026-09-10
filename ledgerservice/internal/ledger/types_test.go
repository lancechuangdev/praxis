package ledger

import "testing"

func TestValidationRejectsInvalidAmountsAndBuckets(t *testing.T) {
	v := PostDeposit{CommandID: "c", DepositID: "d", UserID: "u", AssetID: "a", CustodyPositionID: "p", AmountAtomic: "100", TargetBucket: "available", SourceSystem: "deposit"}
	if err := ValidateDeposit(v); err != nil {
		t.Fatal(err)
	}
	v.TargetBucket = "reserved"
	if err := ValidateDeposit(v); err == nil {
		t.Fatal("expected invalid bucket")
	}
	v.TargetBucket = "available"
	v.AmountAtomic = "1.2"
	if err := ValidateDeposit(v); err == nil {
		t.Fatal("expected non-integer amount rejection")
	}
}
