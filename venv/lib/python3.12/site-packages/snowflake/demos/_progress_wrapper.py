import logging

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Optional


class MultiStepProgress:
    def __init__(
        self,
        console_manager: Any,
        logger: Optional[logging.Logger] = None,
        success_icon: str = "✅",
        error_icon: str = "❌",
    ):
        self.console = console_manager
        self.logger = logger or logging.getLogger(__name__)
        self.success_icon = success_icon
        self.error_icon = error_icon

    @contextmanager
    def progress(self, message: str, error_message: str) -> Generator[None, None, None]:
        """Context manager for progress indication."""
        try:
            if self.console:
                self.console.safe_print(message, color="yellow", end="")

            yield

            if self.console:
                self.console.safe_print(self.success_icon, color="green", bold=True)

        except Exception as e:
            if self.console:
                self.console.safe_print(self.error_icon, color="red", bold=True)

            if self.logger:
                self.logger.error(f"{error_message}: {str(e)}")

            raise
