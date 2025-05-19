import enum
import platform
import sys


# Don't change the constant after this point
PYTHON_VERSION = ".".join(str(v) for v in sys.version_info[:3])
PLATFORM = platform.platform()
SCRIPTS_DIR = "scripts"
DATA_DIR = "data"
RESOURCE_DIR = "resources"
NOTEBOOK_DIR = "notebooks"
ENVIRONMENT_FILE_NAME = "environment.yml"
ENVIRONMENT_FILE_PATH = f"{NOTEBOOK_DIR}/{ENVIRONMENT_FILE_NAME}"
DEMO_NAME_COLUMN = "demo_name"
DEMO_NUM_STEPS_COLUMN = "num_steps"
DEMO_TITLE_COLUMN = "title"
DEMO_MAPPING_COLUMNS = [DEMO_NAME_COLUMN, DEMO_TITLE_COLUMN, DEMO_NUM_STEPS_COLUMN]
DEMO_MAPPING_FILE_NAME = "demo-mappings.csv"
DEMO_MAPPING_FILE_PATH = f"{RESOURCE_DIR}/{DEMO_MAPPING_FILE_NAME}"
DEMO_MAPPING_COLUMN_WIDTHS = [120, 120, 20]
DEMO_DATABASE_NAME = "SNOWFLAKE_DEMO_DB"
DEMO_WAREHOUSE_NAME = "SNOWFLAKE_DEMO_WH"
DEMO_SCHEMA_NAME = "SNOWFLAKE_DEMO_SCHEMA"
DEMO_STAGE_NAME = "SNOWFLAKE_DEMO_STAGE"
DEMO_ROLE_NAME = "SNOWFLAKE_DEMO_ROLE"
SETUP_SCRIPT_NAME = "setup.sql"
SETUP_SCRIPT_PATH = f"{SCRIPTS_DIR}/{SETUP_SCRIPT_NAME}"
TEARDOWN_SCRIPT_NAME = "teardown.sql"
TEARDOWN_SCRIPT_PATH = f"{SCRIPTS_DIR}/{TEARDOWN_SCRIPT_NAME}"
STATIC_DIR_NAME = "static"
STAGES_DIR_NAME = "stages"


@enum.unique
class TelemetryField(enum.Enum):
    KEY_VERSION = "version"
    KEY_PYTHON_VERSION = "python_version"
    KEY_OS = "operating_system"
    KEY_DATA = "data"
    KEY_FUNC_NAME = "function_name"
    KEY_DEMO_NAME = "demo_name"
    KEY_API_SOURCE = "api_source"
