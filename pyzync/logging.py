import os
import sys
import logging
import structlog

log_level = os.environ.get("LOG_LEVEL", "INFO")
    

timestamper = structlog.processors.TimeStamper(fmt="iso")


def extract_from_record(logger, method_name, event_dict):
    """
    Extract thread and process names and add them to the event dict.
    """
    event_dict["logger"] = getattr(logger, "name", "root")
    if not event_dict.get('_from_structlog', False):
        event_dict["logger"] = getattr(event_dict.get("_record"), "name", "unknown")
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

root_logger = logging.getLogger()
root_logger.addHandler(console_handler)
root_logger.setLevel(log_level)

# # Get that specific logger
# urllib3_logger = logging.getLogger("urllib3.connectionpool")

# # Remove any direct handlers it has
# for h in urllib3_logger.handlers[:]:
#     urllib3_logger.removeHandler(h)

# # Ensure it propagates to the root (so structlog can format it)
# urllib3_logger.propagate = True


def get_logger(name: str) -> structlog.BoundLogger:
    return structlog.get_logger(name)
