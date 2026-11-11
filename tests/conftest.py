import pytest

from framework.iternal.http.account import AccountApi
from framework.iternal.http.mail import MailApi
from framework.iternal.kafka.consumer import Consumer
from framework.iternal.kafka.producer import Producer
from framework.helpers.kafka.consumer.register_events import RegisterEventsSubscribers, RegisterEventsErrorsSubscriver
from framework.iternal.rmq.publisher import RmqPublisher


@pytest.fixture(scope="session")
def account() -> AccountApi:
    return AccountApi()


@pytest.fixture(scope="session")
def mail() -> MailApi:
    return MailApi()


@pytest.fixture(scope="session")
def kafka_producer() -> Producer:
    with Producer() as producer:
        yield producer

@pytest.fixture(scope="session")
def rmq_publisher() -> RmqPublisher:
    with RmqPublisher() as rmq_publisher:
        yield rmq_publisher

@pytest.fixture(scope="session")
def register_events_subscriber() -> RegisterEventsSubscribers:
    return RegisterEventsSubscribers()

@pytest.fixture(scope="session")
def register_events_error_subscriber() -> RegisterEventsErrorsSubscriver:
    return RegisterEventsErrorsSubscriver()

@pytest.fixture(scope="session", autouse=True)
def kafka_consumer(
        register_events_subscriber: RegisterEventsSubscribers,
        register_events_error_subscriber: RegisterEventsErrorsSubscriver
) -> Consumer:
    with Consumer(subscribers=[register_events_subscriber, register_events_error_subscriber]) as consumer:
        yield consumer