"""Emit one synthetic signal JSON to stdout. Argv[1] is a serial number."""

import json
import sys
import time
import uuid

n = int(sys.argv[1]) if len(sys.argv) > 1 else 0
print(
    json.dumps(
        {
            "event_id": str(uuid.uuid4()),
            "tenant_id": "ignored",
            "session_id": f"sess_e2e_{n // 10}",
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "device": {
                "os": "iOS",
                "os_version": "17.4.1",
                "ip": f"203.0.113.{n % 255}",
                "fingerprint": f"fp_{n % 4}",
                "battery": 0.8,
                "geo_lat": 37.7749,
                "geo_lon": -122.4194,
            },
            "behavioral": [
                {"kind": "click", "target_elem": "btn_x", "dwell_ms": 200},
                {"kind": "type", "typing_cps": 4.7, "mouse_entropy": 0.62},
            ],
        }
    )
)
