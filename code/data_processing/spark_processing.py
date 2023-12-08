from re import A
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, avg, col, floor, countDistinct, desc
from analytics_class import Analytics
from DBConnection import DB

db = DB()
# Create a cursor
cursor = db.connection.cursor()

spark = SparkSession.builder.appName('ClickstreamAnalytics').getOrCreate()

# Load clickstream data
clickstream_data = spark.read.csv('data/data_stream/Dataset.csv', header=True, inferSchema=True)

# Perform analytics

"""
    Calculate average duration on each page in seconds
"""
Analytics.avg_duration_per_page(clickstream_data).show()

"""
    Count of Sessions per Country
"""

Analytics.count_sessions_per_country(clickstream_data).show()

"""
    Calculate page visit counts
"""
# page_count=Analytics.calculate_page_visit_counts(clickstream_data)
page_count=[
            ["kk",99],
            ["ss",100]
            ]
for row in page_count:
        cursor.execute('''
            INSERT INTO Page_URLCounts (Page_URL, Count)
            VALUES (%s, %s)
        ''', row)
db.connection.commit()
db.connection.close()
# page_count.show()

"""
    Count interaction types
"""

Analytics.count_interaction_types(clickstream_data).show()

"""
    Device type distribution
"""

Analytics.device_type_distribution(clickstream_data).show()

"""
    Page views by country
"""
Analytics.page_views_by_country(clickstream_data).show()

# Stop SparkSession
spark.stop()


select_query = "SELECT * FROM Page_URLCounts"

# Execute the select query
cursor.execute(select_query)

# Fetch all rows and create a pandas DataFrame
columns = [desc[0] for desc in cursor.description]
data = cursor.fetchall()
df = pd.DataFrame(data, columns=columns)

# Display the updated DataFrame
print("Updated Table:")
print(df)
# Close the cursor and connection
cursor.close()
db.connection.close()


# Removed
# # Most common referrer URLs
# common_referrers = clickstream_data \
#     .groupBy('Referrer') \
#     .count() \
#     .orderBy('count', ascending=False)

# # Show results
# common_referrers.show()

#########################################

# # Session duration statistics
# session_duration_stats = clickstream_data \
#     .groupBy('User_ID', 'Session_Start_Time') \
#     .agg((max('Timestamp') - min('Timestamp')).alias('Session_Duration')) \
#     .select(avg('Session_Duration').alias('Avg_Session_Duration'), max('Session_Duration').alias('Max_Session_Duration'), min('Session_Duration').alias('Min_Session_Duration'))

# # Show results
# session_duration_stats.show()