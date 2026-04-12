from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from confluent_kafka import TopicPartition
from rocksdict import Rdict


@dataclass
class PartitionState:
    topic: str
    partition: int
    next_offset: int
    intermediate_state: dict[str, Any]
    updated_at: float


class RocksCheckpointStore:
    def __init__(self, path: str) -> None:
        self._db = Rdict(path)

    @staticmethod
    def _key(topic: str, partition: int) -> str:
        return f"checkpoint:{topic}:{partition}"

    def load(self, topic: str, partition: int) -> PartitionState | None:
        raw_value = self._db.get(self._key(topic, partition))
        if raw_value is None:
            return None
        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8")
        payload = json.loads(raw_value)
        return PartitionState(
            topic=payload["topic"],
            partition=int(payload["partition"]),
            next_offset=int(payload["next_offset"]),
            intermediate_state=dict(payload.get("intermediate_state") or {}),
            updated_at=float(payload["updated_at"]),
        )

    def save(
        self,
        *,
        topic: str,
        partition: int,
        next_offset: int,
        intermediate_state: dict[str, Any],
    ) -> PartitionState:
        state = PartitionState(
            topic=topic,
            partition=partition,
            next_offset=next_offset,
            intermediate_state=intermediate_state,
            updated_at=time.time(),
        )
        self._db[self._key(topic, partition)] = json.dumps(
            {
                "topic": state.topic,
                "partition": state.partition,
                "next_offset": state.next_offset,
                "intermediate_state": state.intermediate_state,
                "updated_at": state.updated_at,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        return state

    def topic_partition_for_commit(self, state: PartitionState) -> TopicPartition:
        return TopicPartition(state.topic, state.partition, state.next_offset)

    def flush(self) -> None:
        self._db.flush(wait=True)
        self._db.flush_wal(sync=True)

    def close(self) -> None:
        self._db.close()
