import matplotlib.pyplot as plt

# Load processed data or results
# For example:
page_visit_counts = [('/home', 100), ('/products', 80), ('/about', 60)]

# Plotting
pages, counts = zip(*page_visit_counts)
plt.bar(pages, counts)
plt.xlabel('Page URL')
plt.ylabel('Visit Count')
plt.title('Page Visit Counts')
plt.xticks(rotation=45)
plt.tight_layout()

# Save or display plot
plt.savefig('page_visit_counts.png')
plt.show()

################

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, countDistinct, desc, max, min

# Initialize SparkSession
spark = SparkSession.builder.appName('ClickstreamAnalytics').getOrCreate()

# Load clickstream data
clickstream_data = spark.read.csv('data/data_stream/Dataset.csv', header=True, inferSchema=True)

# Calculate page visit counts
page_visit_counts = clickstream_data \
    .groupBy('Page_URL') \
    .count() \
    .orderBy('count', ascending=False) \
    .sort('count')

# Calculate average duration per page URL
avg_duration_per_page = clickstream_data \
    .groupBy('Page_URL') \
    .agg(avg('Duration_on_Page_s').alias('avg_duration')) \
    .orderBy('avg_duration', ascending=False)

# Count interaction types
interaction_counts = clickstream_data \
    .groupBy('Interaction_Type') \
    .count() \
    .orderBy('count', ascending=False)

# Device type distribution
device_type_distribution = clickstream_data \
    .groupBy('Device_Type') \
    .count() \
    .orderBy('count', ascending=False)

# Most common browsers
common_browsers = clickstream_data \
    .groupBy('Browser') \
    .count() \
    .orderBy('count', ascending=False)

# Convert Spark DataFrames to Pandas DataFrames for plotting
page_visit_counts_pd = page_visit_counts.toPandas()
avg_duration_per_page_pd = avg_duration_per_page.toPandas()
interaction_counts_pd = interaction_counts.toPandas()
device_type_distribution_pd = device_type_distribution.toPandas()
common_browsers_pd = common_browsers.toPandas()

# Set the style for seaborn plots
sns.set(style="whitegrid")

# Plot page visit counts
plt.figure(figsize=(12, 6))
sns.barplot(x='Page_URL', y='count', data=page_visit_counts_pd)
plt.title('Page Visit Counts')
plt.xlabel('Page URL')
plt.ylabel('Count')
plt.show()

# Plot average duration per page
plt.figure(figsize=(12, 6))
sns.barplot(x='Page_URL', y='avg_duration', data=avg_duration_per_page_pd)
plt.title('Average Duration per Page URL')
plt.xlabel('Page URL')
plt.ylabel('Average Duration (s)')
plt.show()

# Plot interaction counts
plt.figure(figsize=(12, 6))
sns.barplot(x='Interaction_Type', y='count', data=interaction_counts_pd)
plt.title('Interaction Type Counts')
plt.xlabel('Interaction Type')
plt.ylabel('Count')
plt.show()

# Plot device type distribution
plt.figure(figsize=(12, 6))
sns.barplot(x='Device_Type', y='count', data=device_type_distribution_pd)
plt.title('Device Type Distribution')
plt.xlabel('Device Type')
plt.ylabel('Count')
plt.show()

# Plot most common browsers
plt.figure(figsize=(12, 6))
sns.barplot(x='Browser', y='count', data=common_browsers_pd)
plt.title('Common Browsers')
plt.xlabel('Browser')
plt.ylabel('Count')
plt.show()

# Stop SparkSession
spark.stop()