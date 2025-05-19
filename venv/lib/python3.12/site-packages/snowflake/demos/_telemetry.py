import contextlib
import functools
import logging
import platform

from typing import Any, Callable, Optional, TypeVar

from typing_extensions import Concatenate, ParamSpec

from snowflake.connector import SnowflakeConnection
from snowflake.connector.telemetry import (
    TelemetryClient,
    TelemetryData,
)
from snowflake.connector.telemetry import (
    TelemetryField as ConnectorTelemetryField,
)
from snowflake.connector.time_util import get_time_millis
from snowflake.demos._constants import TelemetryField

from .version import __version__ as VERSION


logger = logging.getLogger(__name__)

# Constant to decide whether we are running tests
_called_from_test = False


class ApiTelemetryClient:
    def __init__(self, conn: SnowflakeConnection, is_running_in_notebook: bool = False) -> None:
        self.telemetry: Optional[TelemetryClient] = None if is_running_in_notebook else conn._telemetry
        self.source: str = "snowflake.demos"
        self.version: str = VERSION
        self.python_version: str = platform.python_version()
        self.os: str = platform.system()
        logger.info("telemetry client created for %r, telemetry enabled: %s", conn, bool(self.telemetry))

    def send(self, msg: dict[str, Any], timestamp: Optional[int] = None) -> None:
        if not self.telemetry:
            return
        if not timestamp:
            timestamp = get_time_millis()
        telemetry_data = TelemetryData(message=msg, timestamp=timestamp)
        self.telemetry.try_add_log_to_batch(telemetry_data)

    def send_api_telemetry(self, func_name: str, demo_hanlde: Any) -> None:
        with contextlib.suppress(Exception):
            if not self.telemetry:
                return
            data = {
                TelemetryField.KEY_FUNC_NAME.value: func_name,
                TelemetryField.KEY_DEMO_NAME.value: demo_hanlde._name,
                TelemetryField.KEY_API_SOURCE.value: "demo_handle",
            }
            message = {
                ConnectorTelemetryField.KEY_SOURCE.value: self.source,
                TelemetryField.KEY_VERSION.value: self.version,
                TelemetryField.KEY_PYTHON_VERSION.value: self.python_version,
                TelemetryField.KEY_OS.value: self.os,
                ConnectorTelemetryField.KEY_TYPE.value: "snowflake_demos_api",
                TelemetryField.KEY_DATA.value: data,
            }
            self.send(message)

    def send_top_level_api_telemetry(self, func_name: str, demo_name: str) -> None:
        with contextlib.suppress(Exception):
            if not self.telemetry:
                return
            data = {
                TelemetryField.KEY_FUNC_NAME.value: func_name,
                TelemetryField.KEY_DEMO_NAME.value: demo_name,
                TelemetryField.KEY_API_SOURCE.value: "top_level",
            }
            message = {
                ConnectorTelemetryField.KEY_SOURCE.value: self.source,
                TelemetryField.KEY_VERSION.value: self.version,
                TelemetryField.KEY_PYTHON_VERSION.value: self.python_version,
                TelemetryField.KEY_OS.value: self.os,
                ConnectorTelemetryField.KEY_TYPE.value: "snowflake_demos_api",
                TelemetryField.KEY_DATA.value: data,
            }
            self.send(message)


P = ParamSpec("P")
R = TypeVar("R")


def api_telemetry(func: Callable[Concatenate[Any, P], R]) -> Callable[Concatenate[Any, P], R]:
    @functools.wraps(func)
    def wrap(
        self: Any,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        if _called_from_test:
            raise Exception("Called from test")
        func_name = func.__name__
        logger.debug(
            "calling method %s after submitting telemetry if enabled",
            func_name,
        )
        self._telemetry_client.send_api_telemetry(
            func_name=func_name,
            demo_hanlde=self,
        )
        r = func(self, *args, **kwargs)
        return r

    return wrap
