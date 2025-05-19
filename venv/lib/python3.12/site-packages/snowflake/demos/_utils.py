from __future__ import annotations

import csv
import logging
import os

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from snowflake.core import Root

from rich.table import Table
from sqlparse import split  # type: ignore

from snowflake.connector.cursor import SnowflakeCursor
from snowflake.demos._constants import (
    DATA_DIR,
    DEMO_DATABASE_NAME,
    DEMO_MAPPING_COLUMN_WIDTHS,
    DEMO_MAPPING_COLUMNS,
    DEMO_MAPPING_FILE_PATH,
    DEMO_SCHEMA_NAME,
    DEMO_STAGE_NAME,
    DEMO_WAREHOUSE_NAME,
    ENVIRONMENT_FILE_PATH,
    NOTEBOOK_DIR,
    SETUP_SCRIPT_PATH,
    STAGES_DIR_NAME,
    STATIC_DIR_NAME,
    TEARDOWN_SCRIPT_PATH,
)
from snowflake.demos._demo_connection import DemoConnection
from snowflake.demos._environment_detection import CONSOLE_MANGAER
from snowflake.demos._progress_wrapper import MultiStepProgress


logger = logging.getLogger(__name__)
progress = MultiStepProgress(CONSOLE_MANGAER, logger)

data_directory_file_path = os.path.join(os.path.dirname(__file__), DATA_DIR)


def find_notebook_file(step: int, directory: str) -> str | None:
    """Find a file with the given number followed by an underscore and a name containing alphabets and underscores."""
    if not os.path.exists(directory):
        return None
    for file in os.listdir(directory):
        if file.startswith(str(step - 1) + "_"):
            return file
    return None


def read_demo_mapping_with_cache() -> dict[str, dict[str, str]]:
    """Read the demo mapping CSV file and cache the data in memory."""
    # Read the CSV file
    demo_file_path = os.path.join(os.path.dirname(__file__), DEMO_MAPPING_FILE_PATH)
    demo_map: dict[str, dict[str, str]] = {}
    if not hasattr(read_demo_mapping_with_cache, "cached_data"):
        with open(demo_file_path) as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                demo_name = row[0]
                demo_info = {header[i]: row[i] for i in range(1, len(header))}
                demo_map[demo_name] = demo_info
            read_demo_mapping_with_cache.cached_data = demo_map  # type: ignore[attr-defined]
    return read_demo_mapping_with_cache.cached_data  # type: ignore[attr-defined]


def get_notebook_name_from_file_name(file_name: str, demo_name: str) -> str:
    """Get the notebook name from the file name."""
    return demo_name.replace("-", "_") + "_" + file_name[2:-6]


def print_demo_list() -> None:
    """Print the list of available demos."""
    demos = read_demo_mapping_with_cache()

    CONSOLE_MANGAER.safe_print("List of Demos:", color="cyan")

    table = Table(
        show_header=True,
        header_style="bold magenta",
        safe_box=True,  # Use ASCII characters for borders
        expand=False,  # Don't expand to terminal width if not needed
        show_lines=True,  # This adds lines between rows for better readability
    )

    # Add columns
    for i in range(len(DEMO_MAPPING_COLUMNS)):
        table.add_column(
            DEMO_MAPPING_COLUMNS[i],
            max_width=DEMO_MAPPING_COLUMN_WIDTHS[i],
            overflow="fold",
            style="blue",
            no_wrap=False,
        )

    # Add rows
    for demo_name, demo_info in demos.items():
        table.add_row(
            *[
                demo_name,
                demo_info[DEMO_MAPPING_COLUMNS[1]],
                demo_info[DEMO_MAPPING_COLUMNS[2]],
            ]
        )

    CONSOLE_MANGAER.print_table(table)


def files_upload_succeeded(demo_name: str, root: Root) -> bool:
    """Upload files to stages."""
    demo_directory = os.path.join(data_directory_file_path, demo_name)
    stages_path = os.path.join(demo_directory, STAGES_DIR_NAME)
    if os.path.exists(stages_path):
        for stage_name in os.listdir(stages_path):
            with progress.progress(
                    f"[yellow]Populating Stage[/yellow] [green]@{stage_name}[/green]...",
                    f"Error while populating stage @{stage_name}...",
            ):
                stage_res = root.databases[DEMO_DATABASE_NAME].schemas[DEMO_SCHEMA_NAME].stages[stage_name]
                stage_path = os.path.join(stages_path, stage_name)
                if os.path.isdir(stage_path):
                    for file_name in os.listdir(stage_path):
                        try:
                            local_file_name=os.path.join(stage_path, file_name)
                            stage_res.put(
                                local_file_name=local_file_name,
                                stage_location="",
                                overwrite=True,
                                auto_compress=False,
                            )
                        except Exception as e:
                            logger.error(f"Error while uploading data file {file_name} to stage...", e)
                            return False
    return True


def create_demo_notebooks(demo_name: str, num_steps: int, root: Root) -> bool:
    """Create a default notebook for the given demo."""
    from snowflake.core import CreateMode
    from snowflake.core.notebook import Notebook

    logger.info(f"Creating demo notebooks for {demo_name}")

    stage_handle = root.databases[DEMO_DATABASE_NAME].schemas[DEMO_SCHEMA_NAME].stages[DEMO_STAGE_NAME]

    demo_directory = os.path.join(data_directory_file_path, demo_name)
    notebook_file_directory = os.path.join(demo_directory, NOTEBOOK_DIR)

    environment_file_path = os.path.join(demo_directory, ENVIRONMENT_FILE_PATH)

    CONSOLE_MANGAER.safe_print(
        f"[yellow]Uploading files to stage[/yellow] "
        f"[green]{DEMO_STAGE_NAME}/{demo_name}[/green] [yellow]and creating notebooks...[/yellow]",
        color="yellow",
    )
    if os.path.exists(environment_file_path):
        try:
            stage_handle.put(
                local_file_name=environment_file_path,
                stage_location=f"/{demo_name}",
                overwrite=True,
                auto_compress=False,
            )
        except Exception:
            logger.error(f"Error while uploading file {ENVIRONMENT_FILE_PATH} to stage...")
            raise

    static_files_path = os.path.join(demo_directory, STATIC_DIR_NAME)
    if os.path.exists(static_files_path):
        for file_name in os.listdir(static_files_path):
            try:
                local_file_name=os.path.join(static_files_path, file_name)
                stage_handle.put(
                    local_file_name=local_file_name,
                    stage_location=f"/{demo_name}",
                    overwrite=True,
                    auto_compress=False,
                )
            except Exception as e:
                logger.error(f"Error while uploading static file {file_name} to stage...", e)
                raise

    for i in range(1, num_steps + 1):
        notebook_file = find_notebook_file(i, notebook_file_directory)
        if notebook_file is None:
            continue

        notebook_file_path = os.path.join(
            demo_directory,
            NOTEBOOK_DIR,
            notebook_file,
        )
        try:
            stage_handle.put(
                local_file_name=notebook_file_path,
                stage_location=f"/{demo_name}",
                overwrite=True,
                auto_compress=False,
            )
        except Exception:
            logger.error(f"Error while uploading file {notebook_file} to stage...")
            raise
        notebook_name = get_notebook_name_from_file_name(notebook_file, demo_name)

        notebook = Notebook(
            name=f"{notebook_name}",
            comment=f"Notebook created for Snowflake demo {demo_name}",
            query_warehouse=DEMO_WAREHOUSE_NAME,
            fromLocation=f"@{DEMO_DATABASE_NAME}.{DEMO_SCHEMA_NAME}.{DEMO_STAGE_NAME}/{demo_name}",
            main_file=notebook_file,
        )
        with progress.progress(
            f"[yellow]Creating notebook[/yellow] [green]{notebook_name}[/green]...",
            f"Error while creating notebook {notebook_name}...",
        ):
            notebook_handle = (
                root.databases[DEMO_DATABASE_NAME]
                .schemas[DEMO_SCHEMA_NAME]
                .notebooks.create(notebook, mode=CreateMode.or_replace)
            )
            notebook_handle.add_live_version(from_last=True)
    return True


def cleanup_demo(
    demo_name: str,
    num_steps: int,
    root: Root,
) -> None:
    """Cleanup the demo by deleting the demo notebook created."""
    logger.info(f"Cleaning up demo {demo_name}")
    CONSOLE_MANGAER.safe_print(f"[yellow]Deleting demo[/yellow] [green]{demo_name}[/green]...", color="yellow")
    demo_directory = os.path.join(data_directory_file_path, demo_name)
    notebook_file_directory = os.path.join(demo_directory, NOTEBOOK_DIR)
    with progress.progress(
        "[yellow]Using[/yellow] [red]ACCOUNTADMIN[/red] [yellow]role...[/yellow]",
        "Error while using ACCOUNTADMIN role...",
    ):
        root.connection.cursor().execute("USE ROLE ACCOUNTADMIN")

    for i in range(1, num_steps + 1):
        notebook_file = find_notebook_file(i, notebook_file_directory)
        if notebook_file is None:
            continue

        notebook_name = get_notebook_name_from_file_name(notebook_file, demo_name)

        with progress.progress(
            f"[yellow]Deleting notebook[/yellow] [green]{notebook_name}[/green]...",
            f"Error while deleting notebook {notebook_name}...",
        ):
            root.databases[DEMO_DATABASE_NAME].schemas[DEMO_SCHEMA_NAME].notebooks[notebook_name].drop(
                if_exists=True,
            )


def create_notebook_url_from_demo_name(
    demo_name: str, demo_connection: DemoConnection, step: int = 1
) -> tuple[bool, str]:
    """Create a URL for the notebook in the demo."""
    demo_directory = os.path.join(data_directory_file_path, demo_name)
    notebook_file_directory = os.path.join(demo_directory, NOTEBOOK_DIR)
    notebook_file = find_notebook_file(step, notebook_file_directory)
    if notebook_file is None:
        CONSOLE_MANGAER.safe_print("Error while finding notebook files. Please contact snowflake support", color="red")
        return (False, "")
    notebook_name = get_notebook_name_from_file_name(notebook_file, demo_name)
    return (
        True,
        f"https://app.snowflake.com/{demo_connection.get_organization().lower()}/{demo_connection.get_account().lower()}/#/notebooks/{DEMO_DATABASE_NAME}.{DEMO_SCHEMA_NAME}.{notebook_name.upper()}",
    )


def parse_sql_file(file_path: str) -> list[str]:
    with open(file_path) as file:
        sql_content = file.read()

    # Parse the SQL content
    statements = split(sql_content)

    return statements


def run_setup_commands(cursor: SnowflakeCursor, demo_name: str) -> bool:
    """Get the setup commands from the setup script."""
    demo_directory = os.path.join(data_directory_file_path, demo_name)
    setup_file_path = os.path.join(demo_directory, SETUP_SCRIPT_PATH)
    if not os.path.exists(setup_file_path):
        return True
    commands = parse_sql_file(setup_file_path)

    CONSOLE_MANGAER.safe_print("[yellow]Running setup for this demo[/yellow]...", color="yellow", end="")
    for command in commands:
        try:
            cursor.execute(command)
        except Exception:
            CONSOLE_MANGAER.safe_print("❌", color="red", bold=True)
            logger.error(f"Error while running command for setup: {command}")
            return False
    CONSOLE_MANGAER.safe_print("✅", color="green", bold=True)
    return True


def run_teardown_commands(cursor: SnowflakeCursor, demo_name: str) -> None:
    """Get the teardown commands from the setup script."""
    demo_directory = os.path.join(data_directory_file_path, demo_name)
    teardown_file_path = os.path.join(demo_directory, TEARDOWN_SCRIPT_PATH)
    if not os.path.exists(teardown_file_path):
        return
    commands = parse_sql_file(teardown_file_path)
    with progress.progress(
        "[yellow]Using[/yellow] [red]ACCOUNTADMIN[/red] [yellow]role...[/yellow]",
        "Error while using ACCOUNTADMIN role...",
    ):
        cursor.execute("USE ROLE ACCOUNTADMIN")

    CONSOLE_MANGAER.safe_print("[yellow]Running teardown for this demo[/yellow]...", color="yellow", end="")
    for command in commands:
        try:
            cursor.execute(command)
        except Exception:
            CONSOLE_MANGAER.safe_print("❌", color="red", bold=True)
            logger.error(f"Error while running command for teardown: {command}")
            return
    CONSOLE_MANGAER.safe_print("✅", color="green", bold=True)
    return
