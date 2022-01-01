#!/usr/bin/env python3
import logging
from typing import Optional

import click

from src.app_config import AppConfig
from src.app_logging import AppLogging, LOGGING_CHOICES
from src.runner import Runner
from src.schema_creator import SchemaCreator


_logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--config-file",
    default="/etc/mqtt-pg-logger.yaml",
    help="Config file",
    show_default=True,
    type=click.Path(exists=True),
)
@click.option(
    "--create",
    is_flag=True,
    help="Create database table (if not exists) and create or replace a trigger"
)
@click.option(
    "--log-file",
    help="Log file (if stated journal logging is disabled)"
)
@click.option(
    "--log-level",
    help="Log level",
    type=click.Choice(LOGGING_CHOICES, case_sensitive=False),
)
@click.option(
    "--print-logs",
    is_flag=True,
    help="Print log output to console too"
)
@click.option(
    "--systemd-mode",
    is_flag=True,
    help="Systemd/journald integration: skip timestamp + prints to console"
)
def _main(config_file, create, log_file, log_level, print_logs, systemd_mode):
    try:
        run_service(config_file, create, log_file, log_level, print_logs, systemd_mode)

    except KeyboardInterrupt:
        return 0

    except Exception as ex:
        _logger.exception(ex)
        return 1


def run_service(config_file, create, log_file, log_level, print_logs, systemd_mode):
    """Logs MQTT messages to a Postgres database."""

    creator = None  # type: Optional[SchemaCreator]
    runner = None  # type: Optional[Runner]

    try:
        app_config = AppConfig(config_file)
        AppLogging.configure(
            app_config.get_logging_config(),
            log_file, log_level, print_logs, systemd_mode
        )

        if create:
            creator = SchemaCreator(app_config.get_database_config())
            creator.connect()
            creator.create_schema()
        else:
            runner = Runner(app_config)
            runner.loop()

    finally:
        if creator is not None:
            creator.close()
        if runner is not None:
            runner.close()


if __name__ == '__main__':
    _main()
