from __future__ import annotations

import hashlib
import json
import time
from typing import Any


def fake_process_record(
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    previous_state: dict[str, Any] | None,
) -> dict[str, Any]:
    """Replace this function with real business logic.

    The return value is the intermediate state that should be checkpointed
    after the offset is safely processed.
    """
    previous_count = int((previous_state or {}).get("processed_count", 0))
    value_digest = hashlib.sha256(value or b"").hexdigest()

    try:
        decoded_value = json.loads((value or b"{}").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        decoded_value = {"raw_bytes_length": len(value or b"")}

    # Simulate IO/CPU work without hiding where the real logic belongs.
    time.sleep(0.005)

    return {
        "processed_count": previous_count + 1,
        "last_seen": {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "key": key.decode("utf-8", errors="replace") if key else None,
            "value_digest": value_digest,
            "sample": decoded_value,
        },
    }
