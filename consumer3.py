from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import threading

# Initialize lists to store the incoming data
x_vals = []
y_vals = []

# Lock for synchronizing access to the data lists
data_lock = threading.Lock()

def animate(i):
    with data_lock:
        # Clear the current plot
        plt.cla()
        
        # Plot the data
        plt.plot(x_vals, y_vals, label='Channel 1')

    # Add legend and layout adjustments
    plt.legend(loc='upper left')
    plt.tight_layout()

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
        i=0
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
                
                with data_lock:
                    x_vals.append(i)
                    y_vals.append(data['processedValue'])
                    i+=1
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def main():
    bootstrap_server = "localhost:9092"
    group_id = "consumer-12"
    topic_name = "data"
    consumer_conf = {
        'bootstrap.servers': bootstrap_server,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Read from the beginning if no committed offset  
    }
    consumer = Consumer(consumer_conf)

    # Start the Kafka consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_messages, args=(consumer, topic_name))
    consumer_thread.start()

    # Set up the Matplotlib animation
    fig = plt.figure()
    ani = animation.FuncAnimation(fig, animate, interval=1000, cache_frame_data=False)  # Update every 1000 ms (1 second)

    # Show the plot
    plt.show()

    # Wait for the consumer thread to finish
    consumer_thread.join()

if __name__ == '__main__':
    main()
