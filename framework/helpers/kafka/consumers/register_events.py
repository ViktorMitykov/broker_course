import time

from framework.iternal.kafka.subscriber import Subscriber


class RegisterEventsSubscribers(Subscriber):
    topic: str = "register-events"

    def find_message(self, login: str, timeout: float = 10.0):
        start_time = time.time()

        while time.time() - start_time < timeout:
            message = self.get_message()
            if message.value["login"] == login:
                break
        else:
            raise AssertionError("Email not found")

class RegisterEventsErrorsSubscriver(Subscriber):
    topic: str = "register-events-errors"

    def find_message(self, login: str, error_type: str, timeout: float = 10.0):
        start_time = time.time()

        while time.time() - start_time < timeout:
            message = self.get_message()
            if message.value["input_data"]["login"] == login and message.value["error_type"] == error_type:
                break
        else:
            raise AssertionError("Email not found")