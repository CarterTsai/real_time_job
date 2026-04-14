from __future__ import annotations

import logging
import os

from checkpoint_consumer.config import AppConfig
from checkpoint_consumer.consumer import CheckpointedConsumer


def main() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = AppConfig.from_env()
    CheckpointedConsumer(config).run()


if __name__ == "__main__":
    main()
