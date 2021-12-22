import logging
import os
from typing import List

from src.database import Database
from src.database_utils import DatabaseUtils


_logger = logging.getLogger(__name__)


class SchemaCreator(Database):

    def __init__(self, config):
        super().__init__(config)

        self._auto_commit = True  # creating indices cannot run within a transaction

    def create_schema(self):
        if self._table_name != self.DEFAULT_TABLE_NAME:
            raise ValueError(
                "Cannot create the database schema if an individual table name ({}) is configured. Use the default name ({}) or adapt and "
                "execute the SQL scripts manually!".format(self._table_name, self.DEFAULT_TABLE_NAME)
            )

        # TODO check if table exists

        script = self.get_script_path("table.sql")
        commands = DatabaseUtils.load_commands(script)
        self._execute_commands(commands)

        script = self.get_script_path("convert.sql")
        command = DatabaseUtils.load_as_single_command(script)
        self._execute_commands([command])

        script = self.get_script_path("trigger.sql")
        command = DatabaseUtils.load_as_single_command(script)
        self._execute_commands([command])

        self._connection.commit()

    @classmethod
    def get_script_path(cls, script_name) -> str:
        file_path = os.path.dirname(__file__)
        project_dir = os.path.dirname(file_path)  # go up one time
        return os.path.join(project_dir, "sql", script_name)

    def _execute_commands(self, commands: List[str], auto_commit=False):
        with self._connection.cursor() as cursor:
            for command in commands:
                try:
                    cursor.execute(command)
                except Exception as ex:
                    _logger.error("db-command failed: %s\n%s", ex, command)
                    raise
