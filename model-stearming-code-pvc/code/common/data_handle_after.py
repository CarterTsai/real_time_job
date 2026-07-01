from __future__ import annotations

import logging
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
    """預設推播輸出：scenario 未提供 data_handle_after.py 時的 fallback。
    TODO: 依需求實作 Kafka / MongoDB 輸出。
    """
    previous_count = int((previous_state or {}).get("processed_count", 0))
    LOGGER.info(
        "[common][data_handle_after] topic=%s partition=%s offset=%s",
        topic, partition, offset,
    )
    # TODO: 寫入 Kafka / MongoDB
    return {
        "processed_count": previous_count + 1,
        "last_model_result": model_result,
    }
