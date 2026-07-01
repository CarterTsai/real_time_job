from __future__ import annotations

import logging
import signal
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

from common.base import (
    BatchProcessorProtocol,
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
        batch_processor: BatchProcessorProtocol | None = None,
        data_handle_before: DataHandleBeforeProtocol | None = None,
        contact_policy: ContactPolicyProtocol | None = None,
        data_handle_after: DataHandleAfterProtocol,
    ) -> None:
        self.config = config
        self._processor = processor
        self._batch_processor = batch_processor
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
        # 保護 _pending / _active_states / _records_since_flush 的並行寫入
        self._lock = threading.Lock()

    # ──────────────────────────────────────────────────────────────────────────
    # 主迴圈
    # ──────────────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """主迴圈：一次 consume BATCH_SIZE 筆，按 partition 分組後並行處理。"""
        LOGGER.debug("Starting consumer with config: %s", self.config)
        self._install_signal_handlers()
        self.consumer.subscribe(
            self.config.topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        LOGGER.debug(
            "Consumer started. topics=%s group_id=%s batch_size=%s",
            self.config.topics, self.config.group_id, self.config.batch_size,
        )

        try:
            while self._running:
                messages = self.consumer.consume(
                    num_messages=self.config.batch_size,
                    timeout=self.config.poll_timeout_seconds,
                )
                if not messages:
                    self._flush_if_due(force_time_check=True)
                    continue

                for msg in messages:
                    if msg.error():
                        raise KafkaException(msg.error())

                # 按 (topic, partition) 分組，維持各 partition 內部順序
                partition_groups: dict[tuple[str, int], list[Message]] = defaultdict(list)
                for msg in messages:
                    partition_groups[(msg.topic(), msg.partition())].append(msg)

                if len(partition_groups) == 1:
                    # 單一 partition：不開 thread，省去 overhead
                    self._handle_partition_batch(next(iter(partition_groups.values())))
                else:
                    # 多 partition：各自一個 worker，同時進行
                    with ThreadPoolExecutor(max_workers=len(partition_groups)) as executor:
                        futures = [
                            executor.submit(self._handle_partition_batch, msgs)
                            for msgs in partition_groups.values()
                        ]
                        for f in futures:
                            f.result()  # 重新拋出 worker 內的 exception

                self._flush_if_due()
        finally:
            LOGGER.debug("Shutting down consumer")
            self._flush(force=True)
            self.consumer.close()
            self.store.close()

    # ──────────────────────────────────────────────────────────────────────────
    # 三 Phase 批次處理
    # ──────────────────────────────────────────────────────────────────────────

    def _handle_partition_batch(self, messages: list[Message]) -> None:
        """處理同一 partition 的一批訊息。

        Phase 1 — data_handle_before（循序）
            逐筆讀 previous_state、執行靜態條件檢核與資料 join。
            Skipped 訊息記錄在 records 內，不提前寫 pending，
            確保後續 Phase 3 按原始順序統一寫入。

        Phase 2 — process_batch / process_record（批次 Model 推論）
            有 process_batch 時，將 valid 訊息一次送 Model；
            否則逐筆呼叫 process_record。
            不同 partition 的 Phase 2 由 ThreadPoolExecutor 同時執行。

        Phase 3 — contact_policy + data_handle_after（循序，含 skipped 訊息）
            依原始訊息順序處理每筆（包含 skipped / contact_policy 阻擋的訊息）。
            每筆呼叫前重新讀 latest_prev，確保 processed_count 等計數
            在同一 batch 內正確累積。
        """

        # ── Phase 1: data_handle_before ────────────────────────────────────
        records: list[dict[str, Any]] = []

        for msg in messages:
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            state_key = (topic, partition)

            previous_state = self._get_previous_state(state_key)
            base_kwargs: dict[str, Any] = dict(
                topic=topic,
                partition=partition,
                offset=offset,
                key=msg.key(),
                value=msg.value(),
                previous_state=previous_state,
            )

            join_data: dict[str, Any] = {}
            skipped = False
            if self._data_handle_before is not None:
                should_continue, join_data = self._data_handle_before(**base_kwargs)
                if not should_continue:
                    LOGGER.debug(
                        "data_handle_before skipped. %s-%s offset=%s",
                        topic, partition, offset,
                    )
                    skipped = True

            records.append({
                "state_key": state_key,
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "base_kwargs": base_kwargs,
                "join_data": join_data,
                "skipped": skipped,
                "model_result": None,
            })

        # ── Phase 2: process_batch / process_record ────────────────────────
        valid_records = [r for r in records if not r["skipped"]]

        if valid_records:
            if self._batch_processor is not None:
                # 批次推論：一次送所有 valid 訊息給 Model
                batch_input = [
                    {**r["base_kwargs"], "join_data": r["join_data"]}
                    for r in valid_records
                ]
                model_results = self._batch_processor(batch=batch_input)
            else:
                # 逐筆推論：BATCH_SIZE=1 或 scenario 未實作 process_batch 時的 fallback
                model_results = [
                    self._processor(**r["base_kwargs"], join_data=r["join_data"])
                    for r in valid_records
                ]
            for r, result in zip(valid_records, model_results):
                r["model_result"] = result

        # ── Phase 3: contact_policy + data_handle_after ────────────────────
        # 依原始順序（含 skipped 訊息）統一寫 pending，確保 offset 不遺漏
        for r in records:
            state_key = r["state_key"]
            topic = r["topic"]
            partition = r["partition"]
            offset = r["offset"]

            # 重新讀 latest_prev，取得本 batch 前面訊息的累積結果
            latest_prev = self._get_previous_state(state_key)
            step3_kwargs: dict[str, Any] = {**r["base_kwargs"], "previous_state": latest_prev}

            if r["skipped"]:
                self._store_pending(state_key, topic, partition, offset, latest_prev or {})
                continue

            model_result = r["model_result"]

            if self._contact_policy is not None:
                should_push = self._contact_policy(**step3_kwargs, model_result=model_result)
                if not should_push:
                    LOGGER.debug(
                        "contact_policy blocked. %s-%s offset=%s",
                        topic, partition, offset,
                    )
                    self._store_pending(state_key, topic, partition, offset, latest_prev or {})
                    continue

            new_state = self._data_handle_after(**step3_kwargs, model_result=model_result)
            self._store_pending(state_key, topic, partition, offset, new_state)

    # ──────────────────────────────────────────────────────────────────────────
    # Thread-safe 共用狀態存取
    # ──────────────────────────────────────────────────────────────────────────

    def _get_previous_state(self, state_key: tuple[str, int]) -> dict[str, Any] | None:
        with self._lock:
            pending = self._pending.get(state_key)
            if pending is not None:
                return pending.intermediate_state
            active = self._active_states.get(state_key)
            return active.intermediate_state if active is not None else None

    def _store_pending(
        self,
        state_key: tuple[str, int],
        topic: str,
        partition: int,
        offset: int,
        intermediate_state: dict[str, Any],
    ) -> None:
        with self._lock:
            self._pending[state_key] = PendingCheckpoint(
                topic=topic,
                partition=partition,
                next_offset=offset + 1,
                intermediate_state=intermediate_state,
            )
            self._records_since_flush += 1

    # ──────────────────────────────────────────────────────────────────────────
    # Checkpoint flush
    # ──────────────────────────────────────────────────────────────────────────

    def _flush_if_due(self, *, force_time_check: bool = False) -> None:
        """達到筆數上限或時間間隔時觸發 _flush。"""
        LOGGER.debug("Checking if flush is due, force_time_check=%s", force_time_check)
        record_limit_reached = self._records_since_flush >= self.config.checkpoint_every_records
        time_limit_reached = (time.monotonic() - self._last_flush_time) >= self.config.checkpoint_every_seconds
        if record_limit_reached or time_limit_reached or (force_time_check and self._pending and time_limit_reached):
            self._flush(force=False)

    def _flush(self, *, force: bool) -> None:
        """將所有 pending 狀態寫入 RocksDB，並視設定同步提交 Kafka offset。"""
        LOGGER.debug("Flushing checkpoints, force=%s", force)
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

    # ──────────────────────────────────────────────────────────────────────────
    # Rebalance callbacks
    # ──────────────────────────────────────────────────────────────────────────

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Rebalance 完成分配時：從 RocksDB 讀取 checkpoint，seek 至上次記錄位置後再 assign。"""
        LOGGER.debug("Partitions assigned: %s", partitions)
        partitions_to_assign: list[TopicPartition] = []
        for partition in partitions:
            state = self.store.load(partition.topic, partition.partition)
            if state is not None:
                partition.offset = state.next_offset
                self._active_states[(state.topic, state.partition)] = state
                LOGGER.debug(
                    "Seeking %s-%s to RocksDB checkpoint offset %s",
                    state.topic, state.partition, state.next_offset,
                )
            partitions_to_assign.append(partition)
        consumer.assign(partitions_to_assign)

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Rebalance 正常撤銷 partition 前：強制 flush，清除 pending 與 active state。"""
        LOGGER.debug("Partitions revoked: %s", partitions)
        self._flush(force=True)
        for partition in partitions:
            self._pending.pop((partition.topic, partition.partition), None)
            self._active_states.pop((partition.topic, partition.partition), None)

    def _on_lost(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Rebalance 異常強制收回：直接丟棄 pending 與 active state，不嘗試 flush。"""
        LOGGER.warning("Partitions lost before revoke completed: %s", partitions)
        for partition in partitions:
            self._pending.pop((partition.topic, partition.partition), None)
            self._active_states.pop((partition.topic, partition.partition), None)

    def _install_signal_handlers(self) -> None:
        """SIGINT / SIGTERM → _running=False，優雅停止主迴圈。"""
        def stop(*_: object) -> None:
            self._running = False

        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
