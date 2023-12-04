import pandas as pd

# Load the dataset
data = pd.read_csv('Dataset/Testing/Dataset V3.csv')


### CHATGPT ###

from confluent_kafka import Producer

# Kafka broker configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
}

# Create Kafka producer
producer = Producer(kafka_config)

# Topic to which data will be published
topic = 'clickstream_topic'

# Convert data to JSON and publish to Kafka topic
for _, row in data.iterrows():
    # Convert row to JSON
    message = row.to_json()
    
    # Publish to Kafka topic
    producer.produce(topic, value=message)

# Wait for all messages to be delivered
producer.flush()
