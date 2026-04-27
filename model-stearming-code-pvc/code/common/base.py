from __future__ import annotations

from typing import Any, Protocol


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
    ) -> dict[str, Any]: ...
