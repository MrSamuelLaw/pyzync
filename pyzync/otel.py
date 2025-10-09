import logging
from os import environ
from sys import stdout, stderr
from functools import wraps
from typing import IO, Sequence

import structlog

from opentelemetry import _logs
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, BatchSpanProcessor, ConsoleSpanExporter, SpanExportResult
from opentelemetry.sdk._logs import LogData, LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor, BatchLogRecordProcessor, ConsoleLogExporter, LogExportResult
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter


# define some json exporters
class JsonlConsoleSpanExporter(ConsoleSpanExporter):

    def __init__(self, out: IO = stdout):
        self.out = out

    def export(self, spans: Sequence[ReadableSpan]):
        try:
            for span in spans:
                self.out.write(span.to_json(indent=None) + '\n')
            self.out.flush()
            return SpanExportResult.SUCCESS
        except Exception as e:
            print(e, file=stderr)
            return SpanExportResult.FAILURE


class JsonlConsoleLogExporter(ConsoleLogExporter):

    def __init__(self, out: IO = stdout):
        self.out = out

    def export(self, batch: Sequence[LogData]):  # type: ignore
        try:
            for data in batch:
                self.out.write(data.log_record.to_json(indent=None) + '\n')
            self.out.flush()
            return LogExportResult.SUCCESS
        except Exception as e:
            print(e, file=stderr)
            return LogExportResult.FAILURE


# setup the span exporters
provider = TracerProvider()
trace.set_tracer_provider(provider)
exporters = {
    'otlp': (BatchSpanProcessor, OTLPSpanExporter),
    'jsonl': (SimpleSpanProcessor, JsonlConsoleSpanExporter)
}
for name in environ.get('OTEL_TRACES_EXPORTER', 'jsonl').split(','):
    name = name.strip()
    processor, exporter = exporters[name]
    provider.add_span_processor(processor(exporter()))

# setup the log exporters
provider = LoggerProvider()
_logs.set_logger_provider(provider)
exporters = {
    'otlp': (BatchLogRecordProcessor, OTLPLogExporter),
    'jsonl': (SimpleLogRecordProcessor, JsonlConsoleLogExporter)
}
for name in environ.get('OTEL_LOGS_EXPORTER', 'jsonl').split(','):
    name = name.strip()
    processor, exporter = exporters[name]
    provider.add_log_record_processor(processor(exporter()))


def sanitize_log_record(record: logging.LogRecord) -> logging.LogRecord:
    """
    not really a filter per se, but sanitizes struct logs so that they can 
    be passed safely to the OTEL log exporter, even if they originate from
    structlog.
    
    :param record: The record that is being transformed prior to being emitted.
    """
    try:
        renderer = structlog.dev.ConsoleRenderer()
        # renderer = structlog.processors.JSONRenderer()
        record.msg["logger"] = record._logger.name   # type: ignore
        data = renderer(record._logger, record.name, record.msg) # type: ignore
        record.msg = data
        for k in record.__dict__.keys():
            if k.startswith('_'):
                delattr(record, k)
    except:
        pass
    return record

logger = logging.getLogger()
otel_handler = LoggingHandler()
otel_handler.addFilter(sanitize_log_record)
logger.addHandler(otel_handler)


def with_tracer(tracer: trace.Tracer):
    """
    Decorator that logs a function call with a tracer
    
    :param tracer: Description
    :type tracer: trace.Tracer
    """

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            full_path = f"{func.__module__}.{func.__qualname__}"
            with tracer.start_as_current_span(full_path):
                return func(*args, **kwargs)

        return wrapper

    return decorator
