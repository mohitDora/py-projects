from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from pathlib import Path

#reading json file
# home_dir=Path.home()
# print(home_dir)
# file_path = home_dir / 'data' / 'data.json'

# try:
#     with open(file_path, 'r') as file:
#         data = json.load(file)
# except FileNotFoundError:
#     print(f"The file {file_path} does not exist.")
# except json.JSONDecodeError:
#     print(f"Error decoding JSON from the file {file_path}.")

# print(data["bootstrap_servers"])

# bootstrap_server=data['bootstrap_servers']
# topic_name=data["topic_name"]
# group_id=data["group_id"]

# Configuration settings
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer',
    'auto.offset.reset': 'earliest',
}

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to topic
consumer.subscribe(['my_topic'])

# Poll for messages
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Deserialize the JSON data
            data = json.loads(msg.value().decode('utf-8'))
            print(f"Received message: {data}")

except KeyboardInterrupt:
    pass

finally:
    # Close down consumer to commit final offsets.
    consumer.close()