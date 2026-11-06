"""Shared fixtures for the offline unit tests.

The lean CI install has no psycopg / confluent_kafka. Modules such as
batch_resolve import them at module level, so register inert stand-ins when
the real packages are absent. Tests patch the attributes they use.
"""

import importlib.util
import sys
import types


def _stub_module(name: str, **attrs) -> None:
    if importlib.util.find_spec(name) is None and name not in sys.modules:
        module = types.ModuleType(name)
        module.__dict__.update(attrs)
        sys.modules[name] = module


def _unavailable(*_args, **_kwargs):
    raise RuntimeError("not available in offline unit tests")


_stub_module("psycopg", connect=_unavailable)
_stub_module("confluent_kafka", Producer=_unavailable, Consumer=_unavailable, KafkaError=Exception)
