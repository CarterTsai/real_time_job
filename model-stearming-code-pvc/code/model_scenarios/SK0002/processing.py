from __future__ import annotations

import logging
import os
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
    join_data: dict[str, Any],
) -> dict[str, Any]:
    """單筆 Model 推論：接收 data_handle_before 組裝的 join_data，回傳 model_result。
    BATCH_SIZE=1 或 process_batch 未實作時由 consumer 呼叫。
    """
    scenario = os.environ.get("CONSUMER_PROCESS", "SK0002")
    uid = join_data.get("uid", "")
    cust_id = join_data.get("cust_id", "")
    decoded_value = join_data.get("decoded_value", {})

    LOGGER.info("[%s][%s][%s]--[Model Function][model] call model", scenario, uid, cust_id)

    # TODO: 實際呼叫 Model
    model_id = ""
    model_score = 0.0

    LOGGER.info(
        "[%s][%s][%s]--[Model Function][model] model回傳結果 : id : %s , score : %s",
        scenario, uid, cust_id, model_id, model_score,
    )

    return {
        "uid": uid,
        "cust_id": cust_id,
        "decoded_value": decoded_value,
        "model_id": model_id,
        "model_score": model_score,
    }


def process_batch(*, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """批次 Model 推論：BATCH_SIZE > 1 且此 function 存在時，consumer 改用批次呼叫。
    TODO: 替換為實際支援 batch inference 的 Model API。
    """
    scenario = os.environ.get("CONSUMER_PROCESS", "SK0002")
    LOGGER.info("[%s][process_batch] batch_size=%d", scenario, len(batch))

    # TODO: 呼叫 batch Model API，下方為 fallback（逐筆呼叫）
    return [process_record(**item) for item in batch]
