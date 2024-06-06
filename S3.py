from confluent_kafka import Consumer, KafkaException, KafkaError
import json

def consume_messages(consumer, topic):
    consumer.subscribe([topic])
    print(f'Consuming messages from topic: {topic}')
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'Reached end of partition at offset {msg.offset}')
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            data = json.loads(msg.value().decode('utf-8'))
            print(f'Received Data - {data}')
def main():
    bootstrap_server = input("Enter the bootstrap-server: ")
    topic_name = input("Enter the topic name: ")

    consumer_conf = {
        'bootstrap.servers': bootstrap_server,
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    try:
        consume_messages(consumer, topic_name)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
