import redis
import json
import time
import threading

# Redis connection
r = redis.Redis(host='localhost', port=6379, db=0)

# SQS queue names
email_queue_name = 'email_queue'
sms_queue_name = 'sms_queue'
entity_queue_name = 'entity_queue'

# Email Consumer
def email_consumer():
    while True:
        message_data = r.lpop(email_queue_name)
        if message_data:
            message = json.loads(message_data)
            print(f"Sending email to subscribers: {message['message']}")
            # Implement email sending logic here
        time.sleep(2)  # Simulate a delay between message processing

# SMS Consumer
def sms_consumer():
    while True:
        message_data = r.lpop(sms_queue_name)
        if message_data:
            message = json.loads(message_data)
            print(f"Sending SMS to subscribers: {message['message']}")
        time.sleep(3)  # Simulate a delay between message processing

# Entity Consumer
def entity_consumer():
    while True:
        message_data = r.lpop(entity_queue_name)
        if message_data:
            message = json.loads(message_data)
            print(f"Processing message as an entity: {message['message']}")
            
        time.sleep(4)

if __name__ == "__main__":
    email_consumer_thread = threading.Thread(target=email_consumer)
    sms_consumer_thread = threading.Thread(target=sms_consumer)
    entity_consumer_thread = threading.Thread(target=entity_consumer)

    email_consumer_thread.start()
    sms_consumer_thread.start()
    entity_consumer_thread.start()

    email_consumer_thread.join()
    sms_consumer_thread.join()
    entity_consumer_thread.join()
