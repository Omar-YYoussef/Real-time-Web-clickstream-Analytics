from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType


spark = SparkSession.builder\
    .appName('kafka consumer')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel('WARN')
schema = StructType().add("User ID", IntegerType()).add("Session Start", StringType())\
    .add("Time", StringType()).add("Page URL",StringType).add("Timestamp",StringType)\
    .add("Duration on Page (s)",StringType).add("Interaction Type",StringType).add("Device Type",StringType)\
    .add("Browser",StringType).add("Country",StringType).add("Referrer",StringType)

kafka_topic = "clickstreamV1"
data = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", kafka_topic)\
    .load()\
    .select(from_json(col("value").cast("string"), schema).alias("data"))
data = data.selectExpr("CAST(value AS STRING)")
query = data.writeStream.outputMode("append").format("console").start()
query.awaitTermination()