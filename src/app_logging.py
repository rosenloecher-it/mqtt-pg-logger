import logging
import os
import sys
import logging.handlers


LOGGING_DEFAULT_LOG_LEVEL = "info"
LOGGING_CHOICES = ["debug", "info", "warning", "error"]


LOGGING_JSONSCHEMA = {
    "type": "object",
    "properties": {
        "log_file": {"type": "string", "minLength": 1, "description": "Log file (path)"},
        "log_level": {"type": "string", "enum": LOGGING_CHOICES, "description": "Log level"},
        "max_bytes": {"type": "integer", "minimum": 102400, "description": "Max bytes per log files."},
        "max_count": {"type": "integer", "minimum": 1, "description": "Max count of rolled log files."},
    },
}


class AppLogging:

    @classmethod
    def configure(cls, config_data, log_file, log_level, print_logs, systemd_mode):
        handlers = []

        if not log_file:
            log_file = config_data.get("log_file")

        if not log_level:
            log_level = config_data.get("log_level")
        log_level = cls.parse_log_level(log_level)

        if print_logs is None:
            print_logs = config_data.get("print_logs", False)
        if systemd_mode is None:
            systemd_mode = config_data.get("systemd_mode", False)

        format_with_ts = '%(asctime)s [%(levelname)8s] %(name)s: %(message)s'
        format_no_ts = '[%(levelname)8s] %(name)s: %(message)s'

        if log_file:
            log_dir = os.path.dirname(log_file)
            if not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)

            max_bytes = config_data.get("max_bytes", 1048576)
            max_count = config_data.get("max_count", 5)
            handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=int(max_bytes),
                backupCount=int(max_count)
            )
            formatter = logging.Formatter(format_with_ts)
            handler.setFormatter(formatter)
            handlers.append(handler)

        if systemd_mode:
            log_format = format_no_ts
        else:
            log_format = format_with_ts

        if print_logs or systemd_mode:
            handlers.append(logging.StreamHandler(sys.stdout))

        logging.basicConfig(
            format=log_format,
            level=log_level,
            handlers=handlers
        )

    @classmethod
    def parse_log_level(cls, value):
        value = value or LOGGING_DEFAULT_LOG_LEVEL

        if not isinstance(value, type(logging.INFO)):
            input_value = str(value).lower().strip() if value is not None else value
            if input_value == "debug":
                value = logging.DEBUG
            elif input_value == "info":
                value = logging.INFO
            elif input_value == "warning":
                value = logging.WARNING
            elif input_value == "error":
                value = logging.ERROR
            else:
                value = logging.INFO

        return value
