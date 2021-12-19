import copy
import logging
import os
import pathlib
import sys
from typing import Dict, List

import psycopg
import testing.postgresql
import yaml
from jsonschema import validate
from psycopg.rows import dict_row

from src.mqtt_client import MqttConfKey, MQTT_JSONSCHEMA
from src.schema_creator import SchemaCreator


class SetupTestException(Exception):
    pass


_logger = logging.getLogger(__name__)


class SetupTest:

    TEST_DIR = "__test__"
    DATABASE_DIR = os.path.join(TEST_DIR, "database")

    _logging_inited = False
    _postgresql = None  # type: testing.postgresql.Postgresql

    @classmethod
    def init_logging(cls):
        if not cls._logging_inited:
            cls._logging_inited = True

            logging.basicConfig(
                format='[%(levelname)8s] %(name)s: %(message)s',
                level=logging.DEBUG,
                handlers=[logging.StreamHandler(sys.stdout)]
            )

    @classmethod
    def get_project_dir(cls) -> str:
        file_path = os.path.dirname(__file__)
        out = os.path.dirname(file_path)  # go up one time
        return out

    @classmethod
    def get_test_dir(cls) -> str:
        project_dir = cls.get_project_dir()
        out = os.path.join(project_dir, cls.TEST_DIR)
        return out

    @classmethod
    def get_test_path(cls, relative_path) -> str:
        return os.path.join(cls.get_test_dir(), relative_path)

    @classmethod
    def get_database_dir(cls) -> str:
        project_dir = cls.get_project_dir()
        out = os.path.join(project_dir, cls.DATABASE_DIR)
        return out

    @classmethod
    def ensure_test_dir(cls) -> str:
        return cls.ensure_dir(cls.get_test_dir())

    @classmethod
    def ensure_clean_test_dir(cls) -> str:
        return cls.ensure_clean_dir(cls.get_test_dir())

    @classmethod
    def ensure_database_dir(cls) -> str:
        return cls.ensure_dir(cls.get_database_dir())

    @classmethod
    def ensure_clean_database_dir(cls) -> str:
        return cls.ensure_clean_dir(cls.get_database_dir())

    @classmethod
    def ensure_dir(cls, dir) -> str:
        exists = os.path.exists(dir)

        if exists and not os.path.isdir(dir):
            raise NotADirectoryError(dir)
        if not exists:
            os.makedirs(dir)

        return dir

    @classmethod
    def ensure_clean_dir(cls, dir) -> str:
        if not os.path.exists(dir):
            cls.ensure_dir(dir)
        else:
            cls.clean_dir_recursively(dir)

        return dir

    @classmethod
    def clean_dir_recursively(cls, path_in):
        dir_segments = pathlib.Path(path_in)
        if not dir_segments.is_dir():
            return
        for item in dir_segments.iterdir():
            if item.is_dir():
                cls.clean_dir_recursively(item)
                os.rmdir(item)
            else:
                item.unlink()

    @classmethod
    def get_table_script_path(cls) -> str:
        return os.path.join(cls.get_project_dir(), "sql", "table.sql")

    @classmethod
    def get_trigger_script_path(cls) -> str:
        return os.path.join(cls.get_project_dir(), "sql", "trigger.sql")

    @classmethod
    def init_database(cls, recreate=False, skip_schema_creation=False):
        database_dir = cls.get_database_dir()

        if recreate:
            cls.close_database(shutdown=True)

        if not cls._postgresql:
            cls.ensure_clean_dir(database_dir)
            cls._postgresql = testing.postgresql.Postgresql(base_dir=database_dir)

            if not skip_schema_creation:
                database_params = SetupTest.get_database_params()
                with SchemaCreator(database_params) as schema_creator:
                    schema_creator.create_schema()

    @classmethod
    def close_database(cls, shutdown=False):
        if cls._postgresql:
            if shutdown:
                cls.postgresql.stop()
                cls._postgresql = None

    @classmethod
    def get_database_params(cls, psycopg_naming=False):
        if cls._postgresql:
            params = cls._postgresql.dsn()

            if psycopg_naming:
                db_name_1 = params.get("dbname")
                db_name_2 = params.get("database")
                if not db_name_1 and db_name_2:
                    params["dbname"] = db_name_2
                    del params["database"]

            return params
        else:
            return {}

    @classmethod
    def execute_commands(cls, commands: List[str]):
        if not cls._postgresql:
            raise SetupTestException("Database not initialized!")

        with psycopg.connect(**SetupTest.get_database_params(psycopg_naming=True)) as connection:
            with connection.cursor() as cursor:
                for command in commands:
                    try:
                        cursor.execute(command)
                    except Exception as ex:
                        _logger.error("db-command failed: %s\n%s", ex, command)
                        raise

    @classmethod
    def query_one(cls, query: str):
        if not cls._postgresql:
            raise SetupTestException("Database not initialized!")

        with psycopg.connect(**SetupTest.get_database_params(psycopg_naming=True)) as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(query)
                return cursor.fetchone()

    @classmethod
    def query_all(cls, query: str):
        if not cls._postgresql:
            raise SetupTestException("Database not initialized!")

        with psycopg.connect(**SetupTest.get_database_params(psycopg_naming=True)) as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(query)
                return cursor.fetchall()

    @classmethod
    def valdate_test_config_file(cls, config_data):
        mqtt_schema = copy.deepcopy(MQTT_JSONSCHEMA)
        schema_requires = mqtt_schema["required"]

        for prop_name in [MqttConfKey.SUBSCRIPTIONS, MqttConfKey.SKIP_SUBSCRIPTION_REGEXES]:
            if prop_name in schema_requires:
                schema_requires.remove(prop_name)

        prop_name = MqttConfKey.TEST_SUBSCRIPTION_BASE
        schema_requires.append(prop_name)

        test_config_schema = {
            "type": "object",
            "properties": {
                "mqtt": mqtt_schema,
            },
            "additionalProperties": True,
            "required": ["mqtt"],
        }

        validate(config_data, test_config_schema)

    @classmethod
    def read_test_config(cls) -> Dict:
        config_file = os.path.join(cls.get_project_dir(), "mqtt-pg-logger.yaml")
        if not os.path.isfile(config_file):
            raise FileNotFoundError('test config file ({}) does not exist!'.format(config_file))
        with open(config_file, 'r') as stream:
            config_data = yaml.unsafe_load(stream)

        cls.valdate_test_config_file(config_data)

        return config_data
