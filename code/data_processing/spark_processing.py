"""
This file contains code for performing analytics on clickstream data using Apache Spark.

Note: This file is not a real-time stream. It is meant to demonstrate the analytics in the console.
"""

from pyspark.sql import SparkSession
from analytics_class import Analytics

spark = SparkSession.builder.appName('ClickstreamAnalytics').getOrCreate()
# Load clickstream data
clickstream_data = spark.read.csv('data/data_stream/Dataset.csv', header=True, inferSchema=True)

# Perform analytics

"""
    Calculate average duration on each page in seconds
"""
# Analytics.avg_duration_per_page(clickstream_data).show()

"""
    Count of Sessions per Country
"""

# Analytics.count_sessions_per_country(clickstream_data).show()

"""
    Calculate page visit counts
"""
# Analytics.calculate_page_visit_counts(clickstream_data).show()

"""
    Count interaction types
"""

# Analytics.count_interaction_types(clickstream_data).show()

"""
    Device type distribution
"""

# Analytics.device_type_distribution(clickstream_data).show()

"""
    Page views by country
"""
# Analytics.page_views_by_country(clickstream_data).show()

"""
    Identify popular paths
"""
df=Analytics.identify_popular_paths_non_stream(clickstream_data)
df.show()

# Stop SparkSession
spark.stop()


