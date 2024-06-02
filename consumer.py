from kafka import KafkaConsumer
import json
import ssl

bootstrap_servers = ['kafka-lift-off-mohitdora21.l.aivencloud.com:18482']  # Replace with your Kafka broker(s)
topic_name = 'my_topic'  # Replace with your topic name
group_id = 'my_consumer_group'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    group_id=group_id,
    security_protocol='SSL',
    ssl_cafile='./ca.pem',
    ssl_certfile='./service.cert',
    ssl_keyfile='./service.key',
    auto_offset_reset='earliest',  # Start reading at the beginning of the log if no offset is stored
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))# Decode message values to UTF-8
)

def consume_messages():
    try:
        for message in consumer:
            print(f'Received message: {message.value} from {message.topic} [{message.partition}] at offset {message.offset}')
    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

consume_messages()
