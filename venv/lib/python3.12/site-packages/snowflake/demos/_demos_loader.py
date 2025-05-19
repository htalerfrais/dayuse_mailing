import logging

from typing import Any

from snowflake.demos._demo_handle import DemoHandle


logger = logging.getLogger(__name__)


class SingletonMeta(type):
    _instances: dict[type[Any], Any] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> "DemosLoader":
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class DemosLoader(metaclass=SingletonMeta):
    _loaded_demos: dict[str, DemoHandle] = {}

    def __init__(self) -> None:
        pass

    def get_demo_handle(self, name: str) -> DemoHandle:
        if name not in self._loaded_demos:
            logger.debug("Loading demo %s", name)
            self._loaded_demos[name] = DemoHandle(name)
        if not self._loaded_demos[name]._setup_complete:
            self._loaded_demos[name]._setup_handle()
        return self._loaded_demos[name]

    def cleanup(self) -> None:
        self._loaded_demos = {}

    def invalidate_all_demos(self) -> None:
        for demo in self._loaded_demos.values():
            demo._setup_complete = False
