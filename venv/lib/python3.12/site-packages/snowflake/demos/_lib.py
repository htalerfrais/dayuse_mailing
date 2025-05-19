from typing import Optional

from snowflake.demos._demo_connection import DemoConnection
from snowflake.demos._demo_handle import DemoHandle
from snowflake.demos._demos_loader import DemosLoader
from snowflake.demos._environment_detection import CONSOLE_MANGAER
from snowflake.demos._utils import print_demo_list, read_demo_mapping_with_cache


def help() -> None:
    """Print help message."""
    CONSOLE_MANGAER.safe_print("Welcome to Snowflake Demos!\n", color="cyan")
    CONSOLE_MANGAER.safe_print("To try a single-step demo (num_steps = 1), run:\n", color="cyan")
    CONSOLE_MANGAER.safe_print("from snowflake.demos import load_demo", color="bold magenta")
    CONSOLE_MANGAER.safe_print("load_demo('<demo-name>')\n", color="cyan")
    CONSOLE_MANGAER.safe_print("To try a multi-step demo (num_steps > 1), run:\n", color="cyan")
    CONSOLE_MANGAER.safe_print("from snowflake.demos import load_demo", color="bold magenta")
    CONSOLE_MANGAER.safe_print("demo = load_demo('<demo-name>')", color="cyan")
    CONSOLE_MANGAER.safe_print("demo.show() # to grab the Notebook URL", color="cyan")
    CONSOLE_MANGAER.safe_print("demo.show_next() # to move to the next step and grab the Notebook URL\n", color="cyan")
    CONSOLE_MANGAER.safe_print(
        "To learn more, visit https://docs.snowflake.com/developer-guide/snowflake-python-api/snowflake-python-demos\n",
        color="cyan",
    )
    print_demo_list()


def load_demo(demo_name: str) -> Optional[DemoHandle]:
    """Load the demo with the given name.

    Parameters
    __________
      demo_name: The name of the demo to load.

    Returns
    _______
      The demo handle which can be used perform certain actions on demo.
    """
    demo_mapping = read_demo_mapping_with_cache()
    if demo_name not in demo_mapping.keys():
        CONSOLE_MANGAER.safe_print(f"[red]Demo[/red] [green]'{demo_name}'[/green] [red]not found.[/red]", color="red")
        CONSOLE_MANGAER.safe_print("Please call help() to see the list of available demos.", color="red")
        return None
    demo_handle = DemosLoader().get_demo_handle(demo_name)
    if demo_handle._telemetry_client is not None:
        demo_handle._telemetry_client.send_top_level_api_telemetry("load_demo", demo_name)
    return demo_handle


def teardown() -> None:
    """Teardown all the demo."""
    demo_connection = DemoConnection()
    demo_connection.teardown()
    DemosLoader().invalidate_all_demos()
