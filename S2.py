from confluent_kafka import Producer
import time
import json

bootstrap_server = input("Enter the bootstrap server = ")
topic_name = input("Enter the topic name = ")

conf = {'bootstrap.servers': bootstrap_server}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic - {msg.topic()} partition - {msg.partition()}")

for i in range(10,20):
    data = {
        'value': str(i),
        'timestamp': time.time()
    }
    producer.produce(topic_name, key=f'key{i}'.encode('utf-8'), value=json.dumps(data).encode('utf-8'), callback=delivery_report)
    producer.poll(1)
    
producer.flush()
