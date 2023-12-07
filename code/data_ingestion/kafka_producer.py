# from confluent_kafka import Producer
# import pandas as pd
# from pyspark.sql import SparkSession

# Ss = SparkSession.builder.appName('Kafka Producer').getOrCreate()

# # Kafka broker configuration
# kafka_config = {
#     'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
# }

# # Create Kafka producer
# producer = Producer(kafka_config)

# # Topic to which data will be published
# topic = 'clickstreamV1'

# # Load the dataset
# data = Ss.read.csv('data/Dataset.csv')

# # Convert data to JSON and publish to Kafka topic
# for _, row in data.iterrows():
#     message = row.to_json()  # Convert row to JSON
#     producer.produce(topic, value=message)  # Publish to Kafka topic

# producer.flush()  # Wait for all messages to be delivered

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
schema = StructType().add("User ID", IntegerType()).add("Session Start", StringType())\
    .add("Time", StringType()).add("Page URL",StringType).add("Timestamp",StringType)\
    .add("Duration on Page (s)",StringType).add("Interaction Type",StringType).add("Device Type",StringType)\
    .add("Browser",StringType).add("Country",StringType).add("Referrer",StringType)

streaming_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("path", "../data/Dataset.csv") \
    .load() 
    
df = streaming_df.select(to_json(struct("*")).alias("value"))
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "clickstream") \
    .option("checkpointLocation", "null") \
    .start()
query.awaitTermination()
