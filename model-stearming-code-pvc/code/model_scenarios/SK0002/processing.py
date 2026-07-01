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
    """Model 推論：接收 data_handle_before 組裝的 join_data，呼叫 Model 並回傳結果。"""
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
