from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName('ClickstreamAnalytics').getOrCreate()

# Load clickstream data
clickstream_data = spark.read.csv('clickstream_data.csv', header=True, inferSchema=True)

# Perform analytics or processing
# Example: Calculate page visit counts
page_visit_counts = clickstream_data.groupBy('Page URL').count().orderBy('count', ascending=False)

# Show results
page_visit_counts.show()

# Stop SparkSession
spark.stop()
