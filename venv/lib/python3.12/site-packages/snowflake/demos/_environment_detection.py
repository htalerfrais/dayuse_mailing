import sys

from dataclasses import dataclass
from typing import Optional, Union

from rich import print
from rich.console import Console
from rich.table import Table
from rich.text import Text


@dataclass
class EnvironmentInfo:
    """Stores information about the current environment."""

    is_jupyter: bool
    is_terminal: bool
    is_windows: bool
    color_system: Optional[str]
    terminal_width: int
    terminal_height: int
    is_interactive: bool


class ConsoleManager:
    """Manages Rich console output across different environments."""

    # Define common colors and their fallback HTML/CSS equivalents
    COLOR_MAP = {
        "red": {"rich": "red", "html": "color: #ff0000"},
        "blue": {"rich": "blue", "html": "color: #0000ff"},
        "green": {"rich": "green", "html": "color: #00ff00"},
        "yellow": {"rich": "yellow", "html": "color: #ffff00"},
        "magenta": {"rich": "magenta", "html": "color: #ff00ff"},
        "cyan": {"rich": "cyan", "html": "color: #00ffff"},
        "white": {"rich": "white", "html": "color: #ffffff"},
        "black": {"rich": "black", "html": "color: #000000"},
        "gray": {"rich": "gray", "html": "color: #808080"},
    }

    def __init__(self, force_terminal: Optional[bool] = None) -> None:
        self.env_info = self._detect_environment()
        self.console = self._initialize_console(force_terminal)

    def _detect_environment(self) -> EnvironmentInfo:
        """Detect current environment characteristics."""
        # Check if running in Jupyter
        is_jupyter = "ipykernel" in sys.modules

        # Get console instance for detection
        temp_console = Console()

        return EnvironmentInfo(
            is_jupyter=is_jupyter,
            is_terminal=sys.stdout.isatty(),
            is_windows=sys.platform.startswith("win"),
            color_system=temp_console.color_system,
            terminal_width=temp_console.width,
            terminal_height=temp_console.height,
            is_interactive=sys.stdin.isatty(),
        )

    def _initialize_console(self, force_terminal: Optional[bool]) -> Console:
        """Initialize Rich console with appropriate settings."""
        return Console(
            force_terminal=force_terminal,
            color_system="auto",
            force_jupyter=self.env_info.is_jupyter,
        )

    def safe_print(
        self,
        content: str,
        color: Union[str, None] = None,
        bold: bool = False,
        italic: bool = False,
        underline: bool = False,
        end: str = "\n",  # Added end parameter
    ) -> None:
        """Print with styling and fallbacks for different environments."""
        # Check if content contains Rich markup like [red]text[/red]
        has_rich_markup = "[" in content and "]" in content

        if has_rich_markup and self.env_info.color_system:
            self.console.print(content, end=end)
            return
        elif has_rich_markup:
            content = Text.from_markup(content).plain

        # Build style string for Rich (e.g., "bold red italic")
        style_parts = []
        if color:
            if color.startswith("#"):
                style_parts.append(color)
            else:
                style_parts.append(self.COLOR_MAP.get(color, {}).get("rich", color))
        if bold:
            style_parts.append("bold")
        if italic:
            style_parts.append("italic")
        if underline:
            style_parts.append("underline")
        style_str = " ".join(style_parts)

        # Print based on environment
        if self.env_info.color_system:
            self.console.print(content, style=style_str, end=end)
        elif self.env_info.is_jupyter:
            from IPython.display import HTML, display

            html_style = []
            if color:
                html_style.append(
                    f"color: {color if color.startswith('#') else self.COLOR_MAP.get(color, {}).get('html', color)}"
                )
            if bold:
                html_style.append("font-weight: bold")
            if italic:
                html_style.append("font-style: italic")
            if underline:
                html_style.append("text-decoration: underline")
            display(HTML(f"<pre style='{'; '.join(html_style)}'>{content}</pre>"))
        else:
            print(content, end=end)

    def print_url(self, url: str, display_text: str, color: str = "blue") -> None:
        """Print clickable URL with environment-specific handling."""
        if self.env_info.is_jupyter:
            from IPython.display import HTML, display

            display(HTML(f'<a href="{url}" target="_blank">{display_text}</a>'))
        elif self.env_info.color_system:
            self.console.print(f"[{color}][link={url}]{display_text}[/link][/{color}]")
        else:
            print(f"{display_text}: {url}")

    def print_table(self, table: Table, title: Optional[str] = None) -> None:
        """
        Print table with environment-appropriate formatting.

        Args:
            table: Rich Table object to print
            title: Optional title for the table (if not already set)
        """
        # Set title if provided and not already set
        if title and not table.title:
            table.title = title

        if self.env_info.is_jupyter:  # Jupyter environment
            from IPython.display import HTML, display

            # Convert table to HTML string
            html_str = ""
            with self.console.capture() as capture:
                self.console.print(table)
            html_str = capture.get()

            # Style the table with CSS
            styled_html = f"""
            <style>
                .rich-table {{
                    font-family: monospace;
                    border-collapse: collapse;
                    margin: 10px 0;
                    width: auto;
                }}
                .rich-table td, .rich-table th {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    text-align: left;
                }}
                .rich-table tr:nth-child(even) {{
                    background-color: #f9f9f9;
                }}
                .rich-table th {{
                    background-color: #f5f5f5;
                }}
            </style>
            <div class="rich-table">
            {html_str}
            </div>
            """

            display(HTML(styled_html))
        else:  # Terminal or other environment
            self.console.print(table)


CONSOLE_MANGAER = ConsoleManager()
