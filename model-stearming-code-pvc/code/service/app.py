from __future__ import annotations

import importlib
import logging
import os
import time

from common.base import ProcessorProtocol
from common.config import AppConfig
from common.consumer import CheckpointedConsumer


def _load_processor(process_name: str) -> ProcessorProtocol:
    module = importlib.import_module(f"model_scenarios.{process_name}.processing")
    processor = getattr(module, "process_record", None)
    if processor is None:
        raise ImportError(f"model_scenarios.{process_name}.processing does not define process_record")
    return processor


def main() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.Formatter.converter = lambda *args: time.gmtime(time.time() + 8 * 3600)
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s +0800 %(levelname)s %(name)s %(message)s",
    )

    process_name = os.environ.get("CONSUMER_PROCESS")
    if not process_name:
        raise RuntimeError("CONSUMER_PROCESS environment variable is required (e.g. SL0001)")

    processor = _load_processor(process_name)
    config = AppConfig.from_env()
    CheckpointedConsumer(config, processor).run()


if __name__ == "__main__":
    main()
