from confluent_kafka import Consumer, Producer

# === REDPANDA ===
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'sport-activities'
GROUP_ID = 'activity-consumer-group'

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest' 
}

consumer = Consumer(consumer_conf)
producer = Producer({'bootstrap.servers': KAFKA_BROKER})