from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pymysql
import sys
sys.path.append(r'C:\Users\oyoun\OneDrive\Desktop\COLLEGE\3rd Year\1st Semister\BD\Project\Code\code')
from data_processing.analytics_class import Analytics

# Create a Spark session
spark = SparkSession.builder \
    .appName("Kafka_Consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4") \
    .config("spark.ui.enabled", True) \
    .getOrCreate()

# Display only WARN & ERROR messages
spark.sparkContext.setLogLevel('WARN')

# Define the schema for the dataset
schema = (
    StructType()
    .add("user_id", StringType(), True)
    .add("Session_Start_Time", TimestampType(), True)
    .add("Page_URL", StringType(), True)
    .add("Timestamp", TimestampType(), True)
    .add("Duration_on_Page_s", StringType(), True)
    .add("Interaction_Type", StringType(), True)
    .add("Device_Type", StringType(), True)
    .add("Browser", StringType(), True)
    .add("Country", StringType(), True)
    .add("Referrer", StringType(), True)
)

# Topic from which data will be consumed
kafka_topic = "clickstreamV1"

# Read from Kafka topic
## Number of messages limited to 1000 per trigger
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", kafka_topic) \
    .option("maxOffsetsPerTrigger", 1000) \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data"))

# Select all columns from the dataframe
df = df.select("data.*")

# Perform All analytics
analytics = Analytics(df)
df, column_names, table_name  = analytics.calculate_page_visit_counts()

# Define a function to insert data into the database
def insert_into_db(row):
    try:
        # # Define the connection details for your PHPMyAdmin database
        host = "p3nlmysql47plsk.secureserver.net"
        port = 3306
        database = "Clickstream_DB"
        username = "bigdata"
        password = "hytham123"

        conn = pymysql.connect(host=host, port=port, user=username, password=password, db=database)
        cursor = conn.cursor()

        column_values = []
        # Extract the required columns from the row
        for i in row:
            column_values+=(i)

        # Define the SQL query
        sql_query = f"INSERT INTO {table_name} ("

        # Add column names to the SQL query
        for i in range(len(column_names)):
            sql_query += f"{column_names[i]}, "

        # Remove the trailing comma and space
        sql_query = sql_query[:-2]

        # Complete the SQL query
        sql_query += ") VALUES ("

        # Add column values to the SQL query
        for i in range(len(column_values)):
            sql_query += f"'{column_values[i]}', "

        # Remove the trailing comma and space
        sql_query = sql_query[:-2]

        sql_query += ")"

        print(sql_query)

        # Execute the SQL query
        cursor.execute(sql_query)

        # Commit the changes
        conn.commit()
        conn.close()
    except Exception as e:
        print(e)
        exit(1)


# Write to console & database
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreach(insert_into_db) \
    .start()

# Wait for query termination
query.awaitTermination()