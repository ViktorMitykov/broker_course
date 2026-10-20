import pytest

from framework.iternal.http.account import AccountApi
from framework.iternal.http.mail import MailApi
from framework.iternal.kafka.producer import Producer


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