import kafka
import json
import time

# Kafka configuration details
bootstrap_servers = 'kafka-lift-off-mohitdora21.l.aivencloud.com:18482'  # Replace with your Kafka broker(s)
topic_name = 'my_topic'

# SSL context for the connection
producer = kafka.KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    security_protocol='SSL',
    ssl_cafile='./ca.pem',
    ssl_certfile='./service.cert',
    ssl_keyfile='./service.key',
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    key_serializer=lambda x: json.dumps(x).encode('utf-8')
    # Serialize messages to JSON format and encode as UTF-8
)

# Produce a message
def produce_messages():
    try:
        for i in range(10):
            message = {'key': i, 'value': f'This is message {i}'}
            producer.send(topic_name, value=message)
            print(f'Sent message: {message}')
            time.sleep(1)  # Sleep for a second before sending the next message
    except Exception as e:
        print(f'Error: {e}')
    finally:
        # Close the producer
        producer.close()

produce_messages()
