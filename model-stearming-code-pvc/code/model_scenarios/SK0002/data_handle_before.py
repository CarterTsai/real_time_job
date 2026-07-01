from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any

LOGGER = logging.getLogger(__name__)


def data_handle_before(
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    previous_state: dict[str, Any] | None,
) -> tuple[bool, dict[str, Any]]:
    """資料前置處理：解析訊息、靜態條件檢核、組裝 join_data。
    回傳 (should_continue, join_data)。
    """
    scenario = os.environ.get("CONSUMER_PROCESS", "SK0002")
    uid = str(uuid.uuid4())

    try:
        decoded_value = json.loads((value or b"{}").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        LOGGER.warning("[%s][%s]--[即時資料][kfk] 訊息解析失敗，跳過", scenario, uid)
        return False, {}

    cust_id = decoded_value.get("ACID")
    LOGGER.info("[%s][%s][%s]--[即時資料][kfk] 取得信卡交易資料 : %s", scenario, uid, cust_id, decoded_value)

    # 靜態條件檢核
    # TODO: 查詢靜態可推播名單
    LOGGER.info("[%s][%s][%s]--[靜態條件檢核] 符合可推播名單", scenario, uid, cust_id)

    join_data: dict[str, Any] = {
        "uid": uid,
        "cust_id": cust_id,
        "decoded_value": decoded_value,
    }
    return True, join_data
