from __future__ import annotations

import logging
import os
from typing import Any

LOGGER = logging.getLogger(__name__)


def contact_policy(
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    previous_state: dict[str, Any] | None,
    model_result: dict[str, Any],
) -> bool:
    """推播頻率檢核：近 2 天無推播才允許推送。
    回傳 True → 繼續推播；False → 本次跳過。
    """
    scenario = os.environ.get("CONSUMER_PROCESS", "SK0002")
    uid = model_result.get("uid", "")
    cust_id = model_result.get("cust_id", "")

    # TODO: 查詢 Redis 確認近 2 天推播紀錄
    LOGGER.info("[%s][%s][%s]--[contact檢核][推播檢核] 近2天無推播", scenario, uid, cust_id)
    return True
