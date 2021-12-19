import os
from typing import List


class DatabaseUtils:

    @classmethod
    def load_commands(cls, file: str) -> List[str]:
        """Loads commands from a file"""

        if not os.path.isfile(file):
            raise FileNotFoundError("Script file ({}) not found".format(file))

        with open(file) as f:
            lines = f.readlines()

        return cls._parse_lines_into_commands(lines)

    @classmethod
    def load_as_single_command(cls, file: str) -> str:
        """Loads commands from a file"""

        if not os.path.isfile(file):
            raise FileNotFoundError("Script file ({}) not found".format(file))

        with open(file) as f:
            lines = f.readlines()

        return "\n".join(lines)

    @classmethod
    def split_commands(cls, text: str) -> List[str]:
        text = text.replace("\r", "\n")
        lines = text.split("\n")
        return cls._parse_lines_into_commands(lines)

    @classmethod
    def _parse_lines_into_commands(cls, lines: List[str], strip_comments=True) -> List[str]:
        commands = []
        command = None

        def finish_command():
            nonlocal command
            nonlocal commands
            if command:
                commands.append(command)
            command = None

        for line in lines:
            line_s = line.strip()
            line_r = line.rstrip()
            is_comment = line_s.find("--") == 0
            is_empty = not line_s

            if is_empty:
                continue

            if is_comment and strip_comments:
                continue
            if command is None:
                command = line_r
            else:
                command += "\n" + line_r
            if not is_comment and line_r.endswith(";"):
                finish_command()

        finish_command()

        return commands
