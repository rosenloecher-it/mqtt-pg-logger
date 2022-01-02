import os
import unittest

from src.app_config import AppConfig
from test.setup_test import SetupTest


class TestAppConfig(unittest.TestCase):

    def test_check_config_file_access(self):
        config_file = SetupTest.get_test_path("app_config_file.yaml")

        if os.path.exists(config_file):
            os.remove(config_file)

        with self.assertRaises(FileNotFoundError):
            AppConfig.check_config_file_access(config_file)

        with open(config_file, 'w') as f:
            f.write("dummy config file for file access test.. no yaml needed.")

        os.chmod(config_file, 0o677)
        with self.assertRaises(PermissionError):
            AppConfig.check_config_file_access(config_file)

        os.chmod(config_file, 0o600)
        AppConfig.check_config_file_access(config_file)  # no exception
