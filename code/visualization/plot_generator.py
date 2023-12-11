import matplotlib.pyplot as plt
import seaborn as sns
import sys
sys.path.append(r'C:\Users\oyoun\OneDrive\Desktop\COLLEGE\3rd Year\1st Semister\BD\Project\Code\code')

from data_processing.analytics_class import Analytics
from pyspark.sql import SparkSession

#making session and reading data
spark = SparkSession.builder.appName('Plotting_data').getOrCreate()
clickstream_data = spark.read.csv('data/data_stream/Dataset.csv', header=True, inferSchema=True)

# calculate Page visits
page_visit_counts=Analytics.calculate_page_visit_counts()

# Calculate average duration per page URL
avg_duration_per_page=Analytics.avg_duration_per_page()

# # Count interaction types
interaction_counts=Analytics.count_interaction_types()


# # Device type distribution
device_type_distribution=Analytics.device_type_distribution()

# #Session Per Country
Session_per_Country=Analytics.count_sessions_per_country()


#Page viewing by country
page_view_country=Analytics.page_views_by_country()

# Convert Spark DataFrames to Pandas DataFrames for plotting
page_visit_counts_pd = page_visit_counts.toPandas()
avg_duration_per_page_pd = avg_duration_per_page.toPandas()
interaction_counts_pd = interaction_counts.toPandas()
device_type_distribution_pd = device_type_distribution.toPandas()
Session_per_Country_pd=Session_per_Country.toPandas()
page_view_country_pd=page_view_country.toPandas()


# Set the style for seaborn plots
sns.set(style="whitegrid")

#plot page view by country
plt.figure(figsize=(12, 6))
sns.barplot(x='Country', y='count',data=page_view_country_pd)
plt.title('Page view per Country Counts')
plt.xlabel('Country')
plt.ylabel('Count')
plt.show()

#plot Session per Country
plt.figure(figsize=(12, 6))
sns.barplot(x='Country', y='Session Count', data=Session_per_Country_pd)
plt.title('Session per Country Counts')
plt.xlabel('Country')
plt.ylabel('Count')
plt.show()

# Plot page visit counts
plt.figure(figsize=(12, 6))
sns.barplot(x='Page_URL', y='count', data=page_visit_counts_pd)
plt.title('Page Visit Counts')
plt.xlabel('Page URL')
plt.ylabel('Count')
plt.show()

# Plot average duration per page
plt.figure(figsize=(12, 6))
sns.barplot(x='Page_URL', y='Avg_Duration_minutes', data=avg_duration_per_page_pd)
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

# Stop SparkSession
spark.stop()