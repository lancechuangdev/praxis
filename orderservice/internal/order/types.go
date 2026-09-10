package order

type Request struct {
	RequestID           string `json:"request_id"`
	OrderID             string `json:"order_id"`
	UserID              string `json:"user_id"`
	Symbol              string `json:"symbol"`
	Side                string `json:"side"`
	OrderType           string `json:"order_type"`
	Quantity            string `json:"quantity"`
	Price               string `json:"price,omitempty"`
	ReserveAssetID      string `json:"reserve_asset_id"`
	ReserveAmountAtomic string `json:"reserve_amount_atomic"`
	EnginePartition     int32  `json:"engine_partition"`
}

type Reservation struct {
	ID             string `json:"reservation_id"`
	BalanceVersion int64  `json:"balance_version"`
	Replay         bool   `json:"idempotent_replay"`
}

type Timings struct {
	RiskMS     float64 `json:"risk_ms"`
	ReserveMS  float64 `json:"reserve_ms"`
	MatchingMS float64 `json:"matching_ms"`
	TotalMS    float64 `json:"total_ms"`
}

type Response struct {
	OrderID        string      `json:"order_id"`
	Status         string      `json:"status"`
	Reservation    Reservation `json:"reservation"`
	EngineSequence int64       `json:"engine_sequence"`
	Timings        Timings     `json:"timings"`
}

type MatchResult struct {
	Status         string
	EngineSequence int64
}
