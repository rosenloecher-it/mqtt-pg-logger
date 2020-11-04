#!/usr/bin/env python3
import logging

import click

from src.app_config import AppConfig
from src.app_logging import AppLogging, LOGGING_CHOICES, LOGGING_DEFAULT_LOG_LEVEL
from src.creator import Creator
from src.database import Database
from src.mqtt import Mqtt
from src.runner import Runner

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
    default=LOGGING_DEFAULT_LOG_LEVEL,
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
def _run_service(config_file, create, log_file, log_level, print_logs, systemd_mode):
    """Logs MQTT messages to a Postgres database."""

    mqtt = Mqtt()
    database = Database()

    try:
        # click.echo("config: %s" % config_file)
        # click.echo("log_file: %s" % log_file)
        # click.echo("log_level: %s" % log_level)
        # click.echo("print_logs: %s" % print_logs)
        # click.echo("systemd_mode: %s" % systemd_mode)

        app_config = AppConfig(config_file)
        AppLogging.configure(
            app_config.get_logging_config(),
            log_file, log_level, print_logs, systemd_mode
        )

        database.open(app_config.get_logging_config())

        if create:
            creator = Creator(database)
            creator.create()
        else:
            mqtt.open(app_config.get_mqtt_config())
            runner = Runner(database, mqtt)
            runner.run()

        # click.echo("systemd_mode: %s" % app_config.get_logging_config())

    except KeyboardInterrupt:
        return 0

    except Exception as ex:
        _logger.exception(ex)
        return 1

    finally:
        if mqtt is not None:
            mqtt.close()
        if database is not None:
            database.close()


if __name__ == '__main__':
    _run_service()
