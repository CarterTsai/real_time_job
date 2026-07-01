from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

from common.base import (
    ContactPolicyProtocol,
    DataHandleAfterProtocol,
    DataHandleBeforeProtocol,
    ProcessorProtocol,
)
from common.config import AppConfig
from common.state import PartitionState, RocksCheckpointStore

LOGGER = logging.getLogger(__name__)


@dataclass
class PendingCheckpoint:
    topic: str
    partition: int
    next_offset: int
    intermediate_state: dict[str, Any]


class CheckpointedConsumer:
    def __init__(
        self,
        config: AppConfig,
        processor: ProcessorProtocol,
        *,
        data_handle_before: DataHandleBeforeProtocol | None = None,
        contact_policy: ContactPolicyProtocol | None = None,
        data_handle_after: DataHandleAfterProtocol,
    ) -> None:
        self.config = config
        self._processor = processor
        self._data_handle_before = data_handle_before
        self._contact_policy = contact_policy
        self._data_handle_after = data_handle_after

        self.config.rocksdb_path.mkdir(parents=True, exist_ok=True)
        self.store = RocksCheckpointStore(str(self.config.rocksdb_path))
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.config.bootstrap_servers,
                "group.id": self.config.group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": self.config.auto_offset_reset,
                "enable.partition.eof": False,
            }
        )
        self._running = True
        self._pending: dict[tuple[str, int], PendingCheckpoint] = {}
        self._records_since_flush = 0
        self._last_flush_time = time.monotonic()
        self._active_states: dict[tuple[str, int], PartitionState] = {}

    def run(self) -> None:
        """啟動消費者主迴圈：訂閱 Kafka topic、持續 poll 訊息，直到收到停止信號為止。"""
        LOGGER.debug("Starting consumer with config: %s", self.config)
        self._install_signal_handlers()
        self.consumer.subscribe(
            self.config.topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        LOGGER.debug("Consumer started. topics=%s group_id=%s", self.config.topics, self.config.group_id)

        try:
            while self._running:
                message = self.consumer.poll(self.config.poll_timeout_seconds)
                if message is None:
                    self._flush_if_due(force_time_check=True)
                    continue
                if message.error():
                    raise KafkaException(message.error())
                self._handle_message(message)
                self._flush_if_due()
        finally:
            LOGGER.debug("Shutting down consumer")
            self._flush(force=True)
            self.consumer.close()
            self.store.close()

    def _handle_message(self, message: Message) -> None:
        """四步驟編排：data_handle_before → process_record → contact_policy → data_handle_after。
        任一步驟決定跳過時，仍 commit offset 但保留原 intermediate_state。
        """
        LOGGER.debug("_handle_message. message=%s", message)
        topic = message.topic()
        partition = message.partition()
        offset = message.offset()
        key = message.key()
        value = message.value()
        state_key = (topic, partition)

        pending = self._pending.get(state_key)
        previous_state: dict[str, Any] | None = (
            pending.intermediate_state
            if pending is not None
            else (self._active_states[state_key].intermediate_state if state_key in self._active_states else None)
        )

        base_kwargs: dict[str, Any] = dict(
            topic=topic,
            partition=partition,
            offset=offset,
            key=key,
            value=value,
            previous_state=previous_state,
        )

        # Step 1: data_handle_before（optional）— 靜態條件檢核 + 資料 join
        join_data: dict[str, Any] = {}
        if self._data_handle_before is not None:
            should_continue, join_data = self._data_handle_before(**base_kwargs)
            if not should_continue:
                LOGGER.debug("data_handle_before skipped. topic=%s partition=%s offset=%s", topic, partition, offset)
                self._store_pending(state_key, topic, partition, offset, previous_state or {})
                return

        # Step 2: process_record（required）— Model 推論
        model_result = self._processor(**base_kwargs, join_data=join_data)

        # Step 3: contact_policy（optional）— 推播頻率檢核
        if self._contact_policy is not None:
            should_push = self._contact_policy(**base_kwargs, model_result=model_result)
            if not should_push:
                LOGGER.debug("contact_policy blocked push. topic=%s partition=%s offset=%s", topic, partition, offset)
                self._store_pending(state_key, topic, partition, offset, previous_state or {})
                return

        # Step 4: data_handle_after（required，scenario 優先，fallback 至 common）— 推播輸出
        new_state = self._data_handle_after(**base_kwargs, model_result=model_result)
        self._store_pending(state_key, topic, partition, offset, new_state)

    def _store_pending(
        self,
        state_key: tuple[str, int],
        topic: str,
        partition: int,
        offset: int,
        intermediate_state: dict[str, Any],
    ) -> None:
        self._pending[state_key] = PendingCheckpoint(
            topic=topic,
            partition=partition,
            next_offset=offset + 1,
            intermediate_state=intermediate_state,
        )
        self._records_since_flush += 1

    def _flush_if_due(self, *, force_time_check: bool = False) -> None:
        """判斷是否需要 flush：達到筆數上限或時間間隔時觸發 _flush。"""
        LOGGER.debug("Checking if flush is due, force_time_check=%s", force_time_check)
        record_limit_reached = self._records_since_flush >= self.config.checkpoint_every_records
        time_limit_reached = (time.monotonic() - self._last_flush_time) >= self.config.checkpoint_every_seconds
        if record_limit_reached or time_limit_reached or (force_time_check and self._pending and time_limit_reached):
            self._flush(force=False)

    def _flush(self, *, force: bool) -> None:
        """將所有 pending 狀態寫入 RocksDB，並視設定同步提交 Kafka offset。force=True 時即使無 pending 也更新 flush 時間戳。"""
        LOGGER.debug("Flushing checkpoints , force=%s", force)
        if not self._pending:
            if force:
                self._last_flush_time = time.monotonic()
            return

        committed_states: list[PartitionState] = []
        for pending in list(self._pending.values()):
            state = self.store.save(
                topic=pending.topic,
                partition=pending.partition,
                next_offset=pending.next_offset,
                intermediate_state=pending.intermediate_state,
            )
            self._active_states[(state.topic, state.partition)] = state
            committed_states.append(state)
        self.store.flush()

        self._pending.clear()
        self._records_since_flush = 0
        self._last_flush_time = time.monotonic()
        LOGGER.debug("Flushed %d RocksDB checkpoint(s)", len(committed_states))

        if self.config.commit_kafka_offsets:
            offsets = [self.store.topic_partition_for_commit(state) for state in committed_states]
            self.consumer.commit(offsets=offsets, asynchronous=False)
            LOGGER.debug("Committed %d Kafka offset(s) manually", len(offsets))

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Rebalance 完成分配時呼叫：從 RocksDB 讀取 checkpoint，將 offset seek 至上次記錄位置後再 assign。"""
        LOGGER.debug("Partitions assigned: %s", partitions)
        partitions_to_assign: list[TopicPartition] = []
        for partition in partitions:
            state = self.store.load(partition.topic, partition.partition)
            if state is not None:
                partition.offset = state.next_offset
                self._active_states[(state.topic, state.partition)] = state
                LOGGER.debug(
                    "Seeking %s-%s to RocksDB checkpoint offset %s",
                    state.topic,
                    state.partition,
                    state.next_offset,
                )
            partitions_to_assign.append(partition)
        consumer.assign(partitions_to_assign)

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Rebalance 正常撤銷 partition 前呼叫：強制 flush 未提交狀態，並清除這些 partition 的 pending 與 active state。"""
        LOGGER.debug("Partitions revoked: %s", partitions)
        self._flush(force=True)
        for partition in partitions:
            self._pending.pop((partition.topic, partition.partition), None)
            self._active_states.pop((partition.topic, partition.partition), None)

    def _on_lost(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Rebalance 異常導致 partition 被強制收回時呼叫：直接丟棄 pending 與 active state，不嘗試 flush（避免重複寫入）。"""
        LOGGER.warning("Partitions lost before revoke completed: %s", partitions)
        for partition in partitions:
            self._pending.pop((partition.topic, partition.partition), None)
            self._active_states.pop((partition.topic, partition.partition), None)

    def _install_signal_handlers(self) -> None:
        """註冊 SIGINT 與 SIGTERM 信號處理器，收到信號時將 _running 設為 False 以優雅停止主迴圈。"""
        def stop(*_: object) -> None:
            self._running = False

        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
