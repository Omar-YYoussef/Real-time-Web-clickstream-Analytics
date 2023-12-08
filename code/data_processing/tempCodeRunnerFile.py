from re import A
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, avg, col, floor, countDistinct, desc
from analytics_class import Analytics

spark = SparkSession.builder.appName('ClickstreamAnalytics').getOrCreate()

# Load clickstream data
clickstream_data = spark.read.csv('data/data_stream/Dataset.csv', header=True, inferSchema=True)

# Perform analytics

# """
#     Calculate average duration on each page in seconds
# """
# Analytics.avg_duration_per_page(clickstream_data).show()

# """
#     Count of Sessions per Country
# """

# Analytics.count_sessions_per_country(clickstream_data).show()

# """
#     Calculate page visit counts
# """
# page_count=Analytics.calculate_page_visit_counts(clickstream_data)
# page_count.show()

# """
#     Count interaction types
# """

# Analytics.count_interaction_types(clickstream_data).show()

# """
#     Device type distribution
# """

Analytics.device_type_distribution(clickstream_data).show()

# """
#     Page views by country
# """
# Analytics.page_views_by_country(clickstream_data).show()

# Stop SparkSession
spark.stop()