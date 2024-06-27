import redis
import json

# Redis connection
r = redis.Redis(host='localhost', port=6379, db=0)

# SQS queue names
email_queue_name = 'email_queue'
sms_queue_name = 'sms_queue'
entity_queue_name = 'entity_queue'



def send_message_to_email_queue(message_data):
    if r.llen(email_queue_name) < 100:  
        r.rpush(email_queue_name, json.dumps(message_data))
        print(f"Sent message to email queue")
    else:
        print(f"Email queue is full, message dropped")



def send_message_to_sms_queue(message_data):
    if r.llen(sms_queue_name) < 100:  # Simulate queue size limit
        r.rpush(sms_queue_name, json.dumps(message_data))
        print(f"Sent message to SMS queue")
    else:
        print(f"SMS queue is full, message dropped")



def send_message_to_entity_queue(message_data):
    if r.llen(entity_queue_name) < 100:  # Simulate queue size limit
        r.rpush(entity_queue_name, json.dumps(message_data))
        print(f"Sent message to entity queue")
    else:
        print(f"Entity queue is full, message dropped")
