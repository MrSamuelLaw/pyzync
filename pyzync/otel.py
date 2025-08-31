import logging
from os import environ
from sys import stdout, stderr
from functools import wraps
from typing import IO, Sequence, Optional

from opentelemetry import _logs
from opentelemetry import trace
from opentelemetry.util._once import Once
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, BatchSpanProcessor, ConsoleSpanExporter, SpanExportResult
from opentelemetry.sdk._logs import LogData, LogRecord, LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor, BatchLogRecordProcessor, ConsoleLogExporter, LogExportResult
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter


class JsonlConsoleSpanExporter(ConsoleSpanExporter):
    """Class for exporting jsonl spans from the app for debugging purposes"""

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


# set the supported span exporters if not yet set
_TRACE_EXPORTERS_SET_ONCE = Once()
_TRACE_EXPORTERS_SET: bool = False

if _TRACE_EXPORTERS_SET is False:

    def _set_jsonl_trace_exporter():
        # if its not been setup, try to set it up
        provider = TracerProvider()
        trace.set_tracer_provider(provider)
        # add the exporters to the provider
        EXPORTERS = {
            'otlp': (BatchSpanProcessor, OTLPSpanExporter),
            'jsonl': (SimpleSpanProcessor, JsonlConsoleSpanExporter)
        }
        for name in environ.get('OTEL_TRACES_EXPORTER', 'jsonl').split(','):
            name = name.strip()
            processor, exporter = EXPORTERS[name]
            provider.add_span_processor(processor(exporter()))
        _SPAN_EXPORTERS_SET = True

    did_set = _TRACE_EXPORTERS_SET_ONCE.do_once(_set_jsonl_trace_exporter)


class JsonlConsoleLogExporter(ConsoleLogExporter):
    """Class for exporting jsonl logs from the app for debuggin purposes"""

    def __init__(self, out: IO = stdout):
        self.out = out

    def export(self, batch: Sequence[LogData]):
        try:
            for data in batch:
                self.out.write(data.log_record.to_json(indent=None) + '\n')
            self.out.flush()
            return LogExportResult.SUCCESS
        except Exception as e:
            print(e, file=stderr)
            return LogExportResult.FAILURE


# set the supported logger exporters if not yet set
_LOG_EXPORTERS_SET_ONCE = Once()
_LOG_EXPORTERS_SET: bool = False

if _LOG_EXPORTERS_SET is False:

    def _set_jsonl_log_exporter():
        # if its not been setup, try to set it up
        provider = LoggerProvider()
        _logs.set_logger_provider(provider)
        # add the exporters to the provider
        EXPORTERS = {
            'otlp': (BatchLogRecordProcessor, OTLPLogExporter),
            'jsonl': (SimpleLogRecordProcessor, JsonlConsoleLogExporter)
        }
        for name in environ.get('OTEL_LOGS_EXPORTER', 'jsonl').split(','):
            name = name.strip()
            processor, exporter = EXPORTERS[name]
            provider.add_log_record_processor(processor(exporter()))
        # register the handler
        log_level = environ.get('LOG_LEVEL', 'WARNING')
        log_level = getattr(logging, log_level, logging.INFO)
        logging.basicConfig(level=logging.INFO, handlers=[LoggingHandler()])
        _LOG_EXPORTERS_SET = True

    did_set = _LOG_EXPORTERS_SET_ONCE.do_once(_set_jsonl_log_exporter)


def with_tracer(tracer: trace.Tracer):

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            full_path = f"{func.__module__}.{func.__qualname__}"
            with tracer.start_as_current_span(full_path):
                return func(*args, **kwargs)

        return wrapper

    return decorator
