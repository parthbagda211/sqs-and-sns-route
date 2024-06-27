import redis
import json
import uuid
from sqs import send_message_to_email_queue, send_message_to_sms_queue, send_message_to_entity_queue

# Redis connection
r = redis.Redis(host='localhost', port=6379, db=0)

topic_name = 'my_topic'

def create_topic():
    if not r.exists(topic_name):
        r.set(topic_name, json.dumps({'subscriptions': []}))
        print(f"Created topic: {topic_name}")
        return topic_name
    else:
        print(f"Topic {topic_name} already exists")
        return None

def subscribe_to_topic(protocol, endpoint):

    if r.exists(topic_name):
        topic_data = json.loads(r.get(topic_name))
        subscription_id = str(uuid.uuid4())
        subscription = {
            'id': subscription_id,
            'protocol': protocol,
            'endpoint': endpoint
        }
        topic_data['subscriptions'].append(subscription)
        r.set(topic_name, json.dumps(topic_data))
        print(f"Subscribed {endpoint} to topic {topic_name}")
        return subscription_id
    else:
        print(f"Topic {topic_name} does not exist")
        return None

def publish_message(message, subject=None):
    if r.exists(topic_name):
        topic_data = json.loads(r.get(topic_name))
        subscriptions = topic_data['subscriptions']
        message_id = str(uuid.uuid4())
        message_data = {
            'id': message_id,
            'topic': topic_name,
            'subject': subject,
            'message': message
        }
        for subscription in subscriptions:
            
            print(f"Sending message to {subscription['endpoint']}")
            if subscription['protocol'] == 'email':
                send_message_to_email_queue(message_data)
            elif subscription['protocol'] == 'sms':
                send_message_to_sms_queue(message_data)
            elif subscription['protocol'] == 'entity':
                send_message_to_entity_queue(message_data)
        r.rpush('message_queue', json.dumps(message_data))
        print(f"Published message with ID: {message_id}")
        return message_id
    else:
        print(f"Topic {topic_name} does not exist")
        return None
