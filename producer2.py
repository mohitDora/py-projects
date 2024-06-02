from confluent_kafka import Producer
import time
import json

# Configuration settings
conf = {
    'bootstrap.servers': 'kafka-lift-off-mohitdora21.l.aivencloud.com:18482',
    'security.protocol': 'SSL',
    'ssl.ca.location': './ca.pem',
    'ssl.certificate.location': './service.cert',
    'ssl.key.location': './service.key',
}

# Create Producer instance
producer = Producer(conf)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Produce 10 messages with a 1-second interval
for i in range(10):
    data = {
        'value': f'value{i}',
        'timestamp': time.time()
    }
    producer.produce('my_topic', key=f'key{i}'.encode('utf-8'), value=json.dumps(data).encode('utf-8'), callback=delivery_report)
    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is delivered or failed.
    producer.poll(1)
    # Sleep for 1 second
    time.sleep(1)

# Wait for any outstanding messages to be delivered and delivery report
producer.flush()
input()