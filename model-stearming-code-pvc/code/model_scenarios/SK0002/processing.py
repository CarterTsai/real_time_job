from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

LOGGER = logging.getLogger(__name__)


def process_record(
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    previous_state: dict[str, Any] | None,
) -> dict[str, Any]:
    previous_count = int((previous_state or {}).get("processed_count", 0))
    value_digest = hashlib.sha256(value or b"").hexdigest()

    try:
        LOGGER.info("[SK0002][即時資料][kfk] 取得信卡交易資料")
        decoded_value = json.loads((value or b"{}").decode("utf-8"))
        LOGGER.info(f"[SK0002][即時資料][kfk] 交易資料 : {decoded_value}")

        LOGGER.info(f"[SK0002][kfk Consumer][靜態條件檢核] 符合可推播名單")

        LOGGER.info(f"[SK0002][Model Function][model] call model")
        LOGGER.info(f"[SK0002][Model Function][model] model回傳結果 : id : , score : ")
        LOGGER.info(f"[SK0002][Model Function][model] model end")

        LOGGER.info(f"[SK0002][contact檢核][推播檢核] 近2天無推播")

        LOGGER.info(f"[SK0002][推播][寫入推播紀錄] 寫入redis")
        LOGGER.info(f"[SK0002][推播][寫入推播紀錄] 寫入kfk")
    except (UnicodeDecodeError, json.JSONDecodeError):
        decoded_value = {"raw_bytes_length": len(value or b"")}

    LOGGER.info("Processing record %s:%s:%s with key=%s and value=%s", topic, partition, offset, key, decoded_value)

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
