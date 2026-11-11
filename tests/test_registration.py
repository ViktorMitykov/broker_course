
import time
import uuid

import pytest
from kafka import KafkaProducer

from framework.helpers.kafka.consumer.register_events import RegisterEventsSubscribers, RegisterEventsErrorsSubscriver
from framework.iternal.http.account import AccountApi
from framework.iternal.http.mail import MailApi
from framework.iternal.kafka.producer import Producer
from framework.iternal.rmq.amqconsumer import AmqConsumer
from framework.iternal.rmq.publisher import RmqPublisher


@pytest.fixture
def register_message() -> dict[str, str]:
    base = uuid.uuid4().hex
    return {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123"
    }

@pytest.mark.parametrize("i", range(10))
def test_send_registration(i, account: AccountApi, register_message: dict[str, str]):
    account.register_user(**register_message)


def test_failed_registration(account: AccountApi, mail: MailApi) -> None:
    expected_email = "string@mail.ru"
    account.register_user(login="string", email=expected_email, password="string")
    for _ in range(10):
        response = mail.find_message(query=expected_email)
        if response.json()["total"] == 0:
            break
    else:
        raise AssertionError("Email found")


def test_success_registration(register_events_subscriber: RegisterEventsSubscribers,
                              register_message,
                              account: AccountApi,
                              mail: MailApi) -> None:
    login = register_message["login"]
    account.register_user(**register_message)
    register_events_subscriber.find_message(login=login, timeout=20.0)

    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer(register_message,
                                                  mail: MailApi,
                                                  kafka_producer: KafkaProducer) -> None:
    login = register_message["login"]

    kafka_producer.send('register-events', register_message)

    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
    else:
        raise AssertionError("Email not found")


def test_register_events_error_consumer(mail: MailApi, kafka_producer: KafkaProducer) -> None:
    base = uuid.uuid4().hex + "_viktors"
    message = {
        "input_data": {
            "login": base,
            "email": f"{base}@mail.ru",
            "password": "qwerty12345"
        },
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-83fa06084cd100ac760371ac31079356-76d4e1291befbdcd-01",
            "errors": {
                "Email": [
                    "Taken"
                ],
                "Login": [
                    "Taken"
                ]
            }
        },
        "error_type": "unknown"
    }

    kafka_producer.send('register-events-errors', message)

    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(2)
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer_consumer(
        register_message,
        register_events_subscriber: RegisterEventsSubscribers,
        kafka_producer: Producer
) -> None:
    login = register_message["login"]

    kafka_producer.send('register-events', register_message)

    register_events_subscriber.find_message(login=login)


def test_register_user_invalid_body(account: AccountApi,
                                    register_message,
                                    register_events_subscriber: RegisterEventsSubscribers,
                                    register_events_error_subscriber: RegisterEventsErrorsSubscriver):
    login = ''
    message = register_message
    message['login'] = login

    account.register_user(**message)

    register_events_subscriber.find_message(login)
    register_events_error_subscriber.find_message(login, "validation")


def test_unknown_error_type(register_events_error_subscriber: RegisterEventsErrorsSubscriver,
                            register_message,
                            kafka_producer: KafkaProducer):
    login = ""
    register_message["login"] = login
    message = {
        "input_data": register_message,
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01",
            "errors": {
                "Email": [
                    "Invalid"
                ]
            }
        },
        "error_type": "unknown"
    }

    kafka_producer.send("register-events-errors", message)
    register_events_error_subscriber.find_message(login, "validation")


def test_rmq(rmq_publisher: RmqPublisher, mail: MailApi) -> None:
    address = f"{uuid.uuid4().hex}@mail.ru"
    message = {
        "address": address,
        "subject": "Test message 123123",
        "body": "Test message",
    }
    rmq_publisher.publish(exchange="dm.mail.sending", message=message)

    for _ in range(10):
        response = mail.find_message(query=address)
        if response.json()["total"] > 0:
            break
        time.sleep(2)
    else:
        raise AssertionError("Email not found")

def test_rms_subscriber():
    with AmqConsumer() as consumer:
        messages = consumer.get_message()
        print(messages)

# For push