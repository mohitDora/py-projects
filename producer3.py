from confluent_kafka import Producer

# Configuration for the producer
conf = {
    'bootstrap.servers': 'localhost:9092'
}

# Create Producer instance
producer = Producer(conf)

# Delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Produce a message
topic = 'test_topic'
message = 'Hello, Kafka!'

producer.produce(topic, message.encode('utf-8'), callback=delivery_report)

# Wait for any outstanding messages to be delivered
producer.flush()
