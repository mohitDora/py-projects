from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
import json

def consume_messages(consumer, topic):
    consumer.subscribe([topic])
    print(f'Subscribed to topic: {topic}')
    
    partitions = consumer.assignment()
    if partitions is None:
        print(f"Topic {topic} not found.")
        return

    # Assign the consumer to the topic partitions and seek to the beginning
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)
    for tp in topic_partitions:
        consumer.seek(TopicPartition(tp.topic, tp.partition, 0))
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'Reached end of partition at offset {msg.offset()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                data = json.loads(msg.value().decode('utf-8'))
                print(f'Received Data - {data}')
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def main():
    bootstrap_server = input("Enter the bootstrap server : ")
    group_id = input("Enter the group id : ")
    topic_name = input("Enter the topic name : ")
    consumer_conf = {
        'bootstrap.servers': bootstrap_server,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Read from the beginning if no committed offset  
    }
    consumer = Consumer(consumer_conf)
    consume_messages(consumer, topic_name)
if __name__ == '__main__':
    main()
