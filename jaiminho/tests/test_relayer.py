"""Tests for EventRelayer."""

from datetime import datetime

import dill
import pytest
from dateutil.tz import UTC
from freezegun import freeze_time

from jaiminho.models import Event
from jaiminho.relayer import EventRelayer
from jaiminho.tests.factories import EventFactory


def _dummy_handler(*args, **kwargs):
    """Minimal callable for relay tests."""
    pass


@pytest.mark.django_db
class TestEventRelayerCeleryPayload:
    """Regression test: Celery-style payload with 'args' and 'kwargs' keys must not crash Signal.send()."""

    def test_relay_succeeds_when_payload_contains_args_and_kwargs_keys(self):
        """Event with Celery-style payload (args/kwargs keys) must relay without TypeError."""
        celery_payload = {"task": "my_app.tasks.foo", "args": [1, 2], "kwargs": {"x": 1}}
        event = EventFactory(
            function=dill.dumps(_dummy_handler),
            message=dill.dumps((celery_payload,)),
            kwargs=dill.dumps({"args": [1, 2], "kwargs": {"foo": "bar"}}),
        )

        relayer = EventRelayer()
        with freeze_time("2022-10-31"):
            relayer.relay()

        event.refresh_from_db()
        assert event.sent_at == datetime(2022, 10, 31, tzinfo=UTC)
