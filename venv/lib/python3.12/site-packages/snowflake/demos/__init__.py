import logging

from public import public

from snowflake.demos._constants import PLATFORM, PYTHON_VERSION
from snowflake.demos._demo_handle import DemoHandle
from snowflake.demos._lib import help, load_demo, teardown
from snowflake.demos.version import __version__

from ._logging import simple_file_logging


logger = logging.getLogger(__name__)
logger.info("Snowflake Demos version: %s, on Python %s, on platform: %s", __version__, PYTHON_VERSION, PLATFORM)

public(
    __version__=__version__,
    help=help,
    load_demo=load_demo,
    DemoHandle=DemoHandle,
    teardown=teardown,
    simple_file_logging=simple_file_logging,
)
