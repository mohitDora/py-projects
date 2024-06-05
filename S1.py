from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'your-topic-name'
group_id = 'mygroup'

# Consumer configuration
consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest',  # Read from the beginning if no committed offset
}

# Create a Consumer instance
consumer = Consumer(consumer_conf)

def consume_messages(consumer, topic):
    consumer.subscribe([topic])
    print(f'Subscribed to topic: {topic}')
    
    # Get the partitions for the topic
    partitions = consumer.partitions_for_topic(topic)
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
                    # End of partition event
                    print(f'Reached end of partition at offset {msg.offset()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Print the message key and value
                print(f'Received message: Key: {msg.key().decode("utf-8") if msg.key() else None}, Value: {msg.value().decode("utf-8")}')
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def main():
    # Start consuming messages from the topic
    consume_messages(consumer, topic_name)

if __name__ == '__main__':
    main()
