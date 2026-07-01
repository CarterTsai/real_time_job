from __future__ import annotations

import logging
import os
from typing import Any

LOGGER = logging.getLogger(__name__)


def data_handle_after(
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    previous_state: dict[str, Any] | None,
    model_result: dict[str, Any],
) -> dict[str, Any]:
    """推播輸出：寫入 Redis 推播紀錄、寫入 Kafka 推播 topic，回傳新 checkpoint state。"""
    scenario = os.environ.get("CONSUMER_PROCESS", "SL0001")
    uid = model_result.get("uid", "")
    cust_id = model_result.get("cust_id", "")
    previous_count = int((previous_state or {}).get("processed_count", 0))

    # TODO: 寫入 Redis 推播紀錄
    LOGGER.info("[%s][%s][%s]--[推播][寫入推播紀錄] 寫入redis", scenario, uid, cust_id)

    # TODO: 寫入 Kafka 推播 topic
    LOGGER.info("[%s][%s][%s]--[推播][寫入推播紀錄] 寫入kfk", scenario, uid, cust_id)

    return {
        "processed_count": previous_count + 1,
        "last_model_result": model_result,
    }
