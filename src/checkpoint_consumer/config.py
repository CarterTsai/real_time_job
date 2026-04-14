from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class AppConfig:
    bootstrap_servers: str
    group_id: str
    topics: list[str]
    auto_offset_reset: str
    rocksdb_path: Path
    checkpoint_every_records: int
    checkpoint_every_seconds: float
    commit_kafka_offsets: bool
    poll_timeout_seconds: float

    @classmethod
    def from_env(cls) -> "AppConfig":
        topics = [
            topic.strip()
            for topic in os.getenv("KAFKA_TOPICS", "orders").split(",")
            if topic.strip()
        ]
        if not topics:
            raise ValueError("KAFKA_TOPICS must contain at least one topic")

        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            group_id=os.getenv("KAFKA_GROUP_ID", "rocksdict-checkpoint-consumer"),
            topics=topics,
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            rocksdb_path=Path(os.getenv("ROCKSDB_PATH", "./data/checkpoints")) / socket.gethostname(),
            checkpoint_every_records=int(os.getenv("CHECKPOINT_EVERY_RECORDS", "1000")),
            checkpoint_every_seconds=float(os.getenv("CHECKPOINT_EVERY_SECONDS", "5")),
            commit_kafka_offsets=_env_bool("COMMIT_KAFKA_OFFSETS", False),
            poll_timeout_seconds=float(os.getenv("POLL_TIMEOUT_SECONDS", "1.0")),
        )
