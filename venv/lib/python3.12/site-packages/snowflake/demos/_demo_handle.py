from snowflake.demos._constants import DEMO_NUM_STEPS_COLUMN
from snowflake.demos._demo_connection import DemoConnection
from snowflake.demos._environment_detection import CONSOLE_MANGAER
from snowflake.demos._telemetry import api_telemetry
from snowflake.demos._utils import (
    cleanup_demo,
    create_demo_notebooks,
    create_notebook_url_from_demo_name,
    files_upload_succeeded,
    read_demo_mapping_with_cache,
    run_setup_commands,
    run_teardown_commands,
)


class DemoHandle:
    """Handle to interact with a demo.

    When a demo is loaded using `load_demo`, a `DemoHandle` object is returned.
    This object can be used to interact with the demo.
    Please don't try to instantiate this class directly. Use `load_demo` to get a handle for any demo.

    Examples
    ________
    To load a demo:

    >>> from snowflake.demos import load_demo
    >>> demo = load_demo('<demo_name>')
    >>> demo.show()

    To show the next step in the demo:

    >>> demo.show_next()

    To teardown the objects created during the demo:

    >>> demo.teardown()

    To show a specific step in the demo:

    >>> demo.show(2)

    """

    def __init__(self, name: str):
        self._name = name
        self._initialize_values()

    def _setup_handle(self) -> None:
        self._initialize_values()
        demo_mapping = read_demo_mapping_with_cache()
        demo_info = demo_mapping[self._name]
        self._num_steps = int(demo_info[DEMO_NUM_STEPS_COLUMN])
        # call this to create the root
        self._demo_connection.setup()
        self._telemetry_client = self._demo_connection.get_telemetry_client()
        root = self._demo_connection.get_root()
        if not create_demo_notebooks(self._name, self._num_steps, root):
            return
        if not run_setup_commands(root.connection.cursor(), self._name):
            return
        if not files_upload_succeeded(self._name, root):
            return

        self._setup_complete = True

    def _initialize_values(self) -> None:
        self._current_step = 1
        self._num_steps = 0
        self._demo_connection = DemoConnection()
        self._telemetry_client = None
        self._setup_complete = False

    def _check_is_valid_step(self, step: int) -> bool:
        if not (1 <= step <= self._num_steps):
            CONSOLE_MANGAER.safe_print(
                "[red]Invalid step.[/red]",
                color="red",
            )
            return False
        return True

    def _check_setup(self) -> bool:
        if not self._setup_complete:
            CONSOLE_MANGAER.safe_print(
                f"Setup not complete.\nPlease reload the demo using load_demo('{self._name}')\n"
                "If the issue persists, please contact Snowflake support.",
                color="red",
            )
            return False
        return True

    @api_telemetry
    def show(self, step: int = 1) -> None:
        if not self._check_setup():
            return
        if not self._check_is_valid_step(step):
            CONSOLE_MANGAER.safe_print(f"[red]Please provide a step in range[/red] [1, {self._num_steps}]", color="red")
            return
        status, url = create_notebook_url_from_demo_name(self._name, self._demo_connection, step)
        if status:
            CONSOLE_MANGAER.safe_print(
                f"Showing step {step}.",
                color="cyan",
            )
            CONSOLE_MANGAER.print_url(url, display_text="Ctrl/cmd + click to open the Notebook")

    @api_telemetry
    def show_next(self) -> None:
        if not self._check_setup():
            return
        self._current_step += 1
        if not self._check_is_valid_step(self._current_step):
            self._current_step -= 1
            CONSOLE_MANGAER.safe_print(f"Current step is {self._current_step}", color="red")
            CONSOLE_MANGAER.safe_print("There is no next step available", color="red")
            CONSOLE_MANGAER.safe_print(
                f"[red]Total steps available are in range[/red] [1, {self._num_steps}]", color="red"
            )
            return
        status, url = create_notebook_url_from_demo_name(self._name, self._demo_connection, self._current_step)
        if status:
            CONSOLE_MANGAER.safe_print(
                f"Showing step {self._current_step}.",
                color="cyan",
            )
            CONSOLE_MANGAER.print_url(url, display_text="Ctrl/cmd + click to open the Notebook")

    @api_telemetry
    def teardown(self) -> None:
        """Teardown the demo.

        This will delete all the objects created for showing the demo.
        Please note it might not delete the objects created within the demo.

        Examples
        ________
        To teardown the demo:

        >>> from snowflake.demos import load_demo
        >>> demo = load_demo('<demo_name>')
        >>> demo.teardown()
        """
        if not self._check_setup():
            return
        run_teardown_commands(self._demo_connection.get_root().connection.cursor(), self._name)
        self._setup_complete = False
        cleanup_demo(self._name, self._num_steps, self._demo_connection.get_root())

    def __repr__(self) -> str:
        if not self._check_setup():
            return ""
        status, url = create_notebook_url_from_demo_name(self._name, self._demo_connection, 1)
        if status:
            CONSOLE_MANGAER.safe_print(
                "Showing step 1.",
                color="cyan",
            )
            CONSOLE_MANGAER.print_url(url, display_text="Ctrl/cmd + click to open the Notebook")
        return ""
