import json
import time

from framework.iternal.rmq.amq_consumer import AmqConsumer


class DmMailSending(AmqConsumer):
    exchange = "dm.mail.sending"
    routing_key = "#"

    def find_message(self, login: str, timeout: float = 10.0):
        start_time = time.time()

        while time.time() - start_time < timeout:
            message = self.get_message()
            body = json.loads(message.get("body"))
            print("*"*100)
            print(message)
            if body.get("Login") == login:
                break
        else:
            raise AssertionError("Email not found")
