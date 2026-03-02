import logging
import dill

from jaiminho.constants import PublishStrategyType
from jaiminho.models import Event
from jaiminho.signals import (
    event_published_by_events_relay,
    event_failed_to_publish_by_events_relay,
)
from jaiminho import settings


logger = logging.getLogger(__name__)


def _capture_exception(exception):
    capture_exception = settings.default_capture_exception
    if capture_exception:
        capture_exception(exception)


def _extract_original_func(event):
    fn = dill.loads(event.function)
    original_fn = getattr(fn, "original_func", fn)
    return original_fn


def _send_event_published_signal(sender, message, kwargs):
    """Emit event_published_by_events_relay without splatting payload into Signal.send().

    Celery-style payloads contain 'args' and 'kwargs' keys. Passing **kwargs or
    **payload into Signal.send() would cause: TypeError: got multiple values for
    keyword argument 'args'. We pass the full payload under event_payload only.
    """
    event_published_by_events_relay.send(
        sender=sender,
        event_payload={"message": message, "kwargs": kwargs},
    )


def _send_event_failed_signal(sender, message, kwargs, event, error):
    """Emit event_failed_to_publish_by_events_relay without splatting payload."""
    event_failed_to_publish_by_events_relay.send(
        sender=sender,
        event_payload={"message": message, "kwargs": kwargs},
        event=event,
        error=error,
    )


class EventRelayer:
    def relay(self, stream=None):
        events_qs = Event.objects.filter(sent_at__isnull=True)
        events_qs = events_qs.filter(stream=stream)

        events_qs = events_qs.order_by("created_at")

        if not events_qs:
            logger.info("No failed events found.")
            return

        for event in events_qs:
            args = dill.loads(event.message)
            kwargs = dill.loads(event.kwargs) if event.kwargs else {}

            try:
                original_fn = _extract_original_func(event)
                if isinstance(args, tuple):
                    original_fn(*args, **kwargs)
                else:
                    original_fn(args, **kwargs)

                logger.info(f"JAIMINHO-EVENTS-RELAY: Event sent. Event {event}")

                if settings.delete_after_send:
                    event.delete()
                    logger.info(
                        f"JAIMINHO-EVENTS-RELAY: Event deleted after success send. Event: {event}, Payload: {args}"
                    )
                else:
                    event.mark_as_sent()
                    logger.info(
                        f"JAIMINHO-EVENTS-RELAY: Event marked as sent. Event: {event}, Payload: {args}"
                    )

                _send_event_published_signal(
                    sender=original_fn,
                    message=args,
                    kwargs=kwargs,
                )

            except (ModuleNotFoundError, AttributeError) as e:
                logger.warning(
                    f"JAIMINHO-EVENTS-RELAY: Function does not exist anymore, Event: {event} | Error: {str(e)}"
                )
                _capture_exception(e)

                if self.__stuck_on_error(event):
                    logger.warning(
                        f"JAIMINHO-EVENTS-RELAY: Events relaying are stuck due to failing Event: {event}"
                    )
                    return

            except BaseException as e:
                logger.warning(
                    f"JAIMINHO-EVENTS-RELAY: An error occurred when relaying event: {event} | Error: {str(e)}"
                )
                original_fn = _extract_original_func(event)
                _send_event_failed_signal(
                    sender=original_fn,
                    message=args,
                    kwargs=kwargs,
                    event=event,
                    error=e,
                )
                _capture_exception(e)

                if self.__stuck_on_error(event):
                    logger.warning(
                        f"JAIMINHO-EVENTS-RELAY: Events relaying are stuck due to failing Event: {event}"
                    )
                    return

    def __stuck_on_error(self, event):
        if not event.strategy:
            return settings.publish_strategy == PublishStrategyType.KEEP_ORDER
        return event.strategy == PublishStrategyType.KEEP_ORDER
