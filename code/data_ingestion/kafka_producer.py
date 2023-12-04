from confluent_kafka import Producer
import pandas as pd

# Kafka broker configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
}

# Create Kafka producer
producer = Producer(kafka_config)

# Topic to which data will be published
topic = 'clickstream_topic'

# Load the dataset
data = pd.read_csv('../../data/Dataset.csv')

# Convert data to JSON and publish to Kafka topic
for _, row in data.iterrows():
    message = row.to_json()  # Convert row to JSON
    producer.produce(topic, value=message)  # Publish to Kafka topic

producer.flush()  # Wait for all messages to be delivered
