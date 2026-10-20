import json
import time
import uuid
from kafka import KafkaProducer
from framework.iternal.http.account import AccountApi
from framework.iternal.http.mail import MailApi


def test_failed_registration(account: AccountApi, mail: MailApi) -> None:
    expected_email = "string@mail.ru"
    account.register_user(login="string", email=expected_email, password="string")
    for _ in range(10):
        response = mail.find_message(query=expected_email)
        if response.json()["total"] == 0:
            break
    else:
        raise AssertionError("Email found")


def test_success_registration(account: AccountApi, mail: MailApi) -> None:
    base = uuid.uuid4().hex
    account.register_user(login=base, email=f"{base}@mail.ru", password="string")
    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer(mail: MailApi, kafka_producer: KafkaProducer) -> None:
    base = uuid.uuid4().hex + "_viktor"
    message = {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123"
    }

    kafka_producer.send('register-events', message)

    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
    else:
        raise AssertionError("Email not found")


def test_register_events_error_consumer(mail: MailApi, kafka_producer: KafkaProducer) -> None:
    base = uuid.uuid4().hex + "_viktor"
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


