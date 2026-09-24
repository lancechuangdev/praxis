import http from "k6/http";
import exec from "k6/execution";
import { check } from "k6";
import { Trend, Rate } from "k6/metrics";

const risk = new Trend("order_risk_ms", true);
const reserve = new Trend("order_reserve_ms", true);
const matching = new Trend("order_matching_ms", true);
const total = new Trend("order_total_ms", true);
const failures = new Rate("order_failure_rate");

export const options = {
  scenarios: {
    hot_account: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 100),
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: Number(__ENV.PREALLOCATED_VUS || 100),
      maxVUs: Number(__ENV.MAX_VUS || 5000),
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<50", "p(99)<100"],
    order_failure_rate: ["rate<0.01"],
  },
};

export function setup() {
  return { runId: __ENV.RUN_ID || `run-${Date.now()}` };
}

export default function (data) {
  const unique = `${exec.vu.idInTest}-${exec.scenario.iterationInTest}`;
  const body = JSON.stringify({
    request_id: `hot-request-${data.runId}-${unique}`,
    order_id: `hot-order-${data.runId}-${unique}`,
    user_id: __ENV.USER_ID || "alice",
    symbol: __ENV.SYMBOL || "BTC-USDT",
    side: "buy",
    order_type: "limit",
    quantity: "1",
    price: "70000",
    reserve_asset_id: __ENV.ASSET_ID || "asset_usdt",
    reserve_amount_atomic: __ENV.RESERVE_AMOUNT_ATOMIC || "1",
  });
  const response = http.post(`${__ENV.BASE_URL || "http://localhost:8083"}/v1/orders`, body, { headers: { "Content-Type": "application/json" } });
  const ok = check(response, { "order accepted": (r) => r.status === 202 });
  failures.add(!ok);
  if (ok) {
    const timings = response.json("timings");
    risk.add(timings.risk_ms);
    reserve.add(timings.reserve_ms);
    matching.add(timings.matching_ms);
    total.add(timings.total_ms);
  }
}

