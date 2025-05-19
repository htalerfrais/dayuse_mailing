from __future__ import annotations

import logging

from typing import TYPE_CHECKING, Any, cast


if TYPE_CHECKING:
    from snowflake.core import Root


from snowflake.connector.cursor import SnowflakeCursor
from snowflake.demos._constants import (
    DEMO_DATABASE_NAME,
    DEMO_SCHEMA_NAME,
    DEMO_STAGE_NAME,
    DEMO_WAREHOUSE_NAME,
)
from snowflake.demos._environment_detection import CONSOLE_MANGAER
from snowflake.demos._progress_wrapper import MultiStepProgress
from snowflake.demos._telemetry import ApiTelemetryClient
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.session import Session


logger = logging.getLogger(__name__)
progress = MultiStepProgress(CONSOLE_MANGAER, logger)


class SingletonMeta(type):
    _instances: dict[type[Any], Any] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> DemoConnection:
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class DemoConnection(metaclass=SingletonMeta):
    def __init__(self) -> None:
        self._root: Root | None = None
        self._telemetry_client: ApiTelemetryClient | None = None
        self._organization = None
        self._account = None
        self._is_setup_completed = False

    def setup(self) -> None:
        from snowflake.core import CreateMode
        from snowflake.core.database import Database
        from snowflake.core.schema import Schema
        from snowflake.core.stage import Stage
        from snowflake.core.warehouse import Warehouse

        if not self._root:
            self._root = self._create_root()

        cursor = self._get_cursor()

        with progress.progress(
            "[yellow]Using[/yellow] [red]ACCOUNTADMIN[/red] [yellow]role...[/yellow]",
            "Error while using ACCOUNTADMIN role...",
        ):
            cursor.execute("USE ROLE ACCOUNTADMIN")

        if not self._is_setup_completed:
            logger.info("Creating new database, schema and warehouse for demo setup")

            with progress.progress(
                f"[yellow]Creating Database[/yellow] [green]{DEMO_DATABASE_NAME}[/green]...",
                f"Error while creating database {DEMO_DATABASE_NAME}...",
            ):
                self._root.databases.create(
                    Database(name=DEMO_DATABASE_NAME, comment="Database created for Snowflake demo setup"),
                    mode=CreateMode.or_replace,
                )

            with progress.progress(
                f"[yellow]Creating Schema[/yellow] [green]{DEMO_SCHEMA_NAME}[/green]...",
                f"Error while creating schema {DEMO_SCHEMA_NAME}...",
            ):
                self._root.databases[DEMO_DATABASE_NAME].schemas.create(
                    Schema(name=DEMO_SCHEMA_NAME, comment="Schema created for Snowflake demo setup"),
                    mode=CreateMode.or_replace,
                )

            with progress.progress(
                f"[yellow]Creating Warehouse[/yellow] [green]{DEMO_WAREHOUSE_NAME}[/green]...",
                f"Error while creating warehouse {DEMO_WAREHOUSE_NAME}...",
            ):
                warehouse = Warehouse(
                    name=DEMO_WAREHOUSE_NAME,
                    comment="Warehouse created for Snowflake demo setup",
                    warehouse_size="SMALL",
                    auto_suspend=500,
                )
                self._root.warehouses.create(warehouse, mode=CreateMode.or_replace)

            with progress.progress(
                f"[yellow]Creating Stage[/yellow] [green]{DEMO_STAGE_NAME}[/green]...",
                f"Error while creating stage {DEMO_STAGE_NAME}...",
            ):
                stage = Stage(name=DEMO_STAGE_NAME, comment="Stage created for Snowflake demo setup")
                self._root.databases[DEMO_DATABASE_NAME].schemas[DEMO_SCHEMA_NAME].stages.create(
                    stage,
                    mode=CreateMode.or_replace,
                )

            try:
                organization: SnowflakeCursor | None = self._get_cursor().execute(
                    "SELECT CURRENT_ORGANIZATION_NAME()"
                )
                if organization:
                    self._organization = organization.fetchone()[0]  # type: ignore[index]
                else:
                    CONSOLE_MANGAER.safe_print(
                        "Organization name not found. Make sure the user have sufficient permission.", color="red"
                    )
                    raise ValueError("Organization name not found.")
            except Exception:
                logger.error("Error while fetching organization name...")
                raise

            try:
                account: SnowflakeCursor | None = self._get_cursor().execute("SELECT CURRENT_ACCOUNT_NAME()")
                if account:
                    self._account = account.fetchone()[0]  # type: ignore[index]
                else:
                    CONSOLE_MANGAER.safe_print(
                        "Account name not found. Make sure the user have sufficient permission.", color="red"
                    )
                    raise ValueError("Account name not found.")
            except Exception:
                logger.error("Error while fetching account name...")
                raise
            self._is_setup_completed = True

    def get_root(self) -> Root:
        if self._root is None:
            raise ValueError("Root not set. Please call setup() first.")
        return self._root

    def teardown(self) -> None:
        # invalidate the setup
        self._is_setup_completed = False
        # scenario where teardown is called without load_demo being called first
        if self._root is None:
            self._root = self._create_root()

        if self._telemetry_client is not None:
            self._telemetry_client.send_top_level_api_telemetry("teardown", "all")
        logger.info("Deleting database, schema and warehouse created for demo setup")
        cursor = self._get_cursor()
        with progress.progress(
            "[yellow]Using[/yellow] [red]ACCOUNTADMIN[/red] [yellow]role...[/yellow]",
            "Error while using ACCOUNTADMIN role...",
        ):
            cursor.execute("USE ROLE ACCOUNTADMIN")

        with progress.progress(
            f"[yellow]Dropping Database[/yellow] [green]{DEMO_DATABASE_NAME}[/green]...",
            f"Error while dropping database {DEMO_DATABASE_NAME}...",
        ):
            self._root.databases[DEMO_DATABASE_NAME].drop(if_exists=True)

        with progress.progress(
            f"[yellow]Dropping Warehouse[/yellow] [green]{DEMO_WAREHOUSE_NAME}[/green]...",
            f"Error while dropping warehouse {DEMO_WAREHOUSE_NAME}...",
        ):
            self._root.warehouses[DEMO_WAREHOUSE_NAME].drop(if_exists=True)
        self._root = None

    def _create_root(self) -> Root:
        from snowflake.core import Root
        try:
            import snowbook  # type: ignore # noqa: F401

            session = get_active_session()
            logger.info("Using existing session")
            self._telemetry_client = ApiTelemetryClient(session.connection, True)
            return Root(session)
        except ImportError:
            pass
        logger.info("Creating a new root connection")

        with progress.progress(
            "[yellow]Connecting to[/yellow] [magenta]Snowflake[/magenta]...",
            "Error while connecting to snowflake...",
        ):
            root = Root(Session.builder.create())
        self._telemetry_client = ApiTelemetryClient(root.connection)
        return root

    def get_account(self) -> str:
        if self._account is None:
            raise ValueError("Account not set. Please call setup() first.")
        else:
            return cast(str, self._account)  # type: ignore[unreachable]

    def get_organization(self) -> str:
        if self._organization is None:
            raise ValueError("Organization not set. Please call setup() first.")
        else:
            return cast(str, self._organization)  # type: ignore[unreachable]

    def get_telemetry_client(self) -> ApiTelemetryClient | None:
        return self._telemetry_client

    def _get_cursor(self) -> SnowflakeCursor:
        if self._root is None:
            raise ValueError("Root is not set. Please call setup() first.")
        return self._root.connection.cursor()
