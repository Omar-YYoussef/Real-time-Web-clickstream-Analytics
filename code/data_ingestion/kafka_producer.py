from confluent_kafka import Producer
import pandas as pd
from pyspark.sql import SparkSession

Ss = SparkSession.builder.appName('Kafka Producer').getOrCreate()

# Kafka broker configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
}

# Create Kafka producer
producer = Producer(kafka_config)

# Topic to which data will be published
topic = 'clickstreamV1'

# Load the dataset
data = Ss.read.csv('data/Dataset.csv')

# Convert data to JSON and publish to Kafka topic
for _, row in data.iterrows():
    message = row.to_json()  # Convert row to JSON
    producer.produce(topic, value=message)  # Publish to Kafka topic

producer.flush()  # Wait for all messages to be delivered
