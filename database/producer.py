import time
import random
import threading
from sns import publish_message

def produce_messages():
    while True:
        message = f"This is a test message from the producer: {random.randint(1, 100)}"
        subject = "Test Message"
        publish_message(message, subject)
        time.sleep(5)

if __name__ == "__main__":
    producer = threading.Thread(target=produce_messages)
    producer.start()
    producer.join()
