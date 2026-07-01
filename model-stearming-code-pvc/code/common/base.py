from __future__ import annotations

from typing import Any, Protocol


class DataHandleBeforeProtocol(Protocol):
    def __call__(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        key: bytes | None,
        value: bytes | None,
        previous_state: dict[str, Any] | None,
    ) -> tuple[bool, dict[str, Any]]: ...
    # returns (should_continue, join_data)
    # should_continue=False → 跳過後續步驟，直接 commit offset


class ProcessorProtocol(Protocol):
    def __call__(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        key: bytes | None,
        value: bytes | None,
        previous_state: dict[str, Any] | None,
        join_data: dict[str, Any],
    ) -> dict[str, Any]: ...
    # returns model_result


class BatchProcessorProtocol(Protocol):
    def __call__(
        self,
        *,
        batch: list[dict[str, Any]],
    ) -> list[dict[str, Any]]: ...
    # batch items: {topic, partition, offset, key, value, previous_state, join_data}
    # returns list of model_results, 順序與 batch 相同
    # 有此 Protocol 實作時，consumer 改用批次推論取代逐筆呼叫 ProcessorProtocol


class ContactPolicyProtocol(Protocol):
    def __call__(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        key: bytes | None,
        value: bytes | None,
        previous_state: dict[str, Any] | None,
        model_result: dict[str, Any],
    ) -> bool: ...
    # returns should_push
    # False → 不推播，跳過 data_handle_after，直接 commit offset


class DataHandleAfterProtocol(Protocol):
    def __call__(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        key: bytes | None,
        value: bytes | None,
        previous_state: dict[str, Any] | None,
        model_result: dict[str, Any],
    ) -> dict[str, Any]: ...
    # returns new_intermediate_state 寫入 RocksDB checkpoint
