from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import min, max, avg
# Initialize SparkSession
spark = SparkSession.builder.appName('ClickstreamAnalytics').getOrCreate()

# Load clickstream data
clickstream_data = spark.read.csv('data/data_stream/Dataset.csv', header=True, inferSchema=True)

# Perform analytics or processing
average_duration = clickstream_data.select(avg("Duration_on_Page_s")).collect()[0][0]
print(f"Average Duration on Page: {average_duration} seconds")

sessions_per_country = clickstream_data.groupBy("Country").count().withColumnRenamed("count", "Session Count")
sessions_per_country.show()
# Calculate page visit counts
page_visit_counts = clickstream_data \
                        .groupBy('Page_URL') \
                        .count() \
                        .orderBy('count', ascending=False) \
                        .sort('count')

page_visit_counts.show()

# Calculate average duration per page URL
avg_duration_per_page = clickstream_data \
    .groupBy('Page_URL') \
    .avg('Duration_on_Page_s') \
    .orderBy('avg(Duration_on_Page_s)', ascending=False)

# Count interaction types
interaction_counts = clickstream_data \
    .groupBy('Interaction_Type') \
    .count() \
    .orderBy('count', ascending=False)

interaction_counts.show()

# Device type distribution
device_type_distribution = clickstream_data \
    .groupBy('Device_Type') \
    .count() \
    .orderBy('count', ascending=False)

device_type_distribution.show()

# Most common browsers
common_browsers = clickstream_data \
    .groupBy('Browser') \
    .count() \
    .orderBy('count', ascending=False)

common_browsers.show()

# Define the number of top users to retrieve
top_users_count = 10

# Calculate top users by session count
top_users_by_sessions = clickstream_data \
    .groupBy('User_ID') \
    .agg(countDistinct('Session_Start_Time').alias('Session_Count')) \
    .orderBy(desc('Session_Count')) \
    .limit(top_users_count)

# Show results
top_users_by_sessions.show()

# Page views by country
page_views_by_country = clickstream_data \
    .groupBy('Country') \
    .count() \
    .orderBy('count', ascending=False)

# Show results
page_views_by_country.show()

# Most common referrer URLs
common_referrers = clickstream_data \
    .groupBy('Referrer') \
    .count() \
    .orderBy('count', ascending=False)

# Show results
common_referrers.show()

# Session duration statistics
session_duration_stats = clickstream_data \
    .groupBy('User_ID', 'Session_Start_Time') \
    .agg((max('Timestamp') - min('Timestamp')).alias('Session_Duration')) \
    .select(avg('Session_Duration').alias('Avg_Session_Duration'), max('Session_Duration').alias('Max_Session_Duration'), min('Session_Duration').alias('Min_Session_Duration'))

# Show results
session_duration_stats.show()

# Stop SparkSession
spark.stop()
