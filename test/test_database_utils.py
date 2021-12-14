import unittest

from src.database_utils import DatabaseUtils
from test.setup_test import SetupTest


class TestDatabaseUtils(unittest.TestCase):

    def test_load_commands(self):

        lines = [
            " ",
            " -- comment filtered out",
            "1 ",
            " 2 ",
            "3; ",
            "",
            "4;",
            "5; ",
            "",
            "6; ",
        ]

        script_path = SetupTest.get_test_path("temp.sql")
        with open(script_path, 'w') as f:
            f.write("\n".join(lines))

        commands = DatabaseUtils.load_commands(script_path)

        self.assertEqual(len(commands), 4)
        self.assertEqual(commands[0], "1\n 2\n3;")
        self.assertEqual(commands[1], "4;")
        self.assertEqual(commands[2], "5;")
        self.assertEqual(commands[3], "6;")
