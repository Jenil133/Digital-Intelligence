// k6 load script for di-collector POST /v1/signals.
//
// Target from task.md 1.7: 10k req/s sustained, p99 < 50ms on a single pod.
//
// Run:
//   BASE_URL=http://localhost:8080 TOKEN=dev-token k6 run loadtest/ingest.js
//
// For the 10k rps target on bare-metal, use --vus 200 --rps 10000 or rely on
// the constant-arrival-rate scenario below.

import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const TOKEN = __ENV.TOKEN || 'dev-token';

export const options = {
  scenarios: {
    sustain_10k: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RPS || 10000),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 200,
      maxVUs: 1000,
    },
  },
  thresholds: {
    http_req_duration: ['p(99)<50', 'p(95)<25'],
    http_req_failed: ['rate<0.001'],
  },
};

function buildSignal() {
  const now = new Date().toISOString();
  return JSON.stringify({
    event_id: uuidv4(),
    tenant_id: 'ignored',
    session_id: `sess_${__VU}_${Math.floor(__ITER / 50)}`,
    ts: now,
    device: {
      os: 'iOS',
      os_version: '17.4.1',
      ip: `203.0.113.${__VU % 255}`,
      fingerprint: `fp_${__VU}`,
      battery: 0.8,
    },
    behavioral: [
      { kind: 'click', target_elem: 'btn_x', dwell_ms: 200 },
    ],
  });
}

const params = {
  headers: {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${TOKEN}`,
  },
};

export default function () {
  const res = http.post(`${BASE_URL}/v1/signals`, buildSignal(), params);
  check(res, {
    'status 202': (r) => r.status === 202,
  });
}
