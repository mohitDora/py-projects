from confluent_kafka import Producer
import time
import json
import random
import string

def generate_sample_data():
    time_id = int(time.time() * 1000)  # Using current time in milliseconds
    parameter = ''.join(random.choices(string.ascii_uppercase, k=10))
    
    processed_value = random.uniform(-1000, 1000)  # Random float between -1000 and 1000
    int_value = int(processed_value)  # Convert to an integer for binary, hex, oct

    binary_value = bin(int_value)  # Binary representation
    hex_value = hex(int_value)  # Hexadecimal representation
    oct_value = oct(int_value)  # Octal representation

    data = {
        "timeID": time_id,
        "parameter": parameter,
        "binaryValue": binary_value,
        "hexValue": hex_value,
        "octValue": oct_value,
        "processedValue": processed_value
    }
    return data



# Configuration settings
conf = {
    'bootstrap.servers': 'localhost:9092',   
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
    data = generate_sample_data()
    producer.produce('my_topic', key=f'key{i}'.encode('utf-8'), value=json.dumps(data).encode('utf-8'), callback=delivery_report)
    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is delivered or failed.
    producer.poll(1)
    # Sleep for 1 second
    time.sleep(1)

# Wait for any outstanding messages to be delivered and delivery report
producer.flush()