import sys
import logging
import structlog
from os import environ

console_log_level = environ.get("CONSOLE_LOG_LEVEL", "INFO")

timestamper = structlog.processors.TimeStamper(fmt="iso")


def extract_from_record(logger, method_name, event_dict):
    """
    Extract thread and process names and add them to the event dict.
    """
    event_dict["logger"] = getattr(logger, "name", "root")
    if not event_dict.get('_from_structlog', False):
        name = getattr(event_dict.get("_record"), "name", "unknown")
        event_dict["logger"] = name

    return event_dict


structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(console_log_level)
console_handler.setFormatter(
    structlog.stdlib.ProcessorFormatter(processors=[
        timestamper,
        structlog.stdlib.add_log_level,
        extract_from_record,
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        structlog.dev.ConsoleRenderer(
            colors=True,
            exception_formatter=structlog.dev.plain_traceback,
        ),
    ],))

otel_log_level = environ.get('OTEL_LOG_LEVEL', 'INFO')
root_log_level = min(getattr(logging, otel_log_level, 0), getattr(logging, console_log_level, 0))
root_logger = logging.getLogger()
root_logger.addHandler(console_handler)
root_logger.setLevel(root_log_level)

# 3rd party loggers get set to warning
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("dropbox").setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.BoundLogger:
    return structlog.get_logger(name)
