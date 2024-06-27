from sns import create_topic, subscribe_to_topic
from producer import produce_messages

if __name__ == "__main__":
    create_topic()
    subscribe_to_topic('email', 'your_email@example.com')
    subscribe_to_topic('sms', '+1234567890')
    subscribe_to_topic('entity', 'entity_processor')

    produce_messages()
