from __future__ import annotations

import importlib
import logging
import os
import time
from typing import Any

from common.base import (
    BatchProcessorProtocol,
    ContactPolicyProtocol,
    DataHandleAfterProtocol,
    DataHandleBeforeProtocol,
    ProcessorProtocol,
)
from common.config import AppConfig
from common.consumer import CheckpointedConsumer


def _load_required(process_name: str, module_name: str, func_name: str) -> Any:
    module = importlib.import_module(f"model_scenarios.{process_name}.{module_name}")
    func = getattr(module, func_name, None)
    if func is None:
        raise ImportError(f"model_scenarios.{process_name}.{module_name} does not define {func_name}")
    return func


def _load_optional(process_name: str, module_name: str, func_name: str) -> Any | None:
    try:
        module = importlib.import_module(f"model_scenarios.{process_name}.{module_name}")
        return getattr(module, func_name, None)
    except ModuleNotFoundError:
        return None


def _load_data_handle_after(process_name: str) -> DataHandleAfterProtocol:
    handler = _load_optional(process_name, "data_handle_after", "data_handle_after")
    if handler is not None:
        return handler
    module = importlib.import_module("common.data_handle_after")
    return module.data_handle_after


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

    processor: ProcessorProtocol = _load_required(process_name, "processing", "process_record")
    batch_processor: BatchProcessorProtocol | None = _load_optional(
        process_name, "processing", "process_batch"
    )
    data_handle_before: DataHandleBeforeProtocol | None = _load_optional(
        process_name, "data_handle_before", "data_handle_before"
    )
    contact_policy: ContactPolicyProtocol | None = _load_optional(
        process_name, "contact_policy", "contact_policy"
    )
    data_handle_after: DataHandleAfterProtocol = _load_data_handle_after(process_name)

    logger = logging.getLogger(__name__)
    logger.info(
        "Loaded modules for %s — batch_processor=%s data_handle_before=%s "
        "contact_policy=%s data_handle_after=%s",
        process_name,
        batch_processor is not None,
        data_handle_before is not None,
        contact_policy is not None,
        getattr(data_handle_after, "__module__", "?"),
    )

    config = AppConfig.from_env()
    CheckpointedConsumer(
        config,
        processor,
        batch_processor=batch_processor,
        data_handle_before=data_handle_before,
        contact_policy=contact_policy,
        data_handle_after=data_handle_after,
    ).run()


if __name__ == "__main__":
    main()
