from pyspark.sql.functions import window, avg, col, floor, current_timestamp, pandas_udf, collect_list, lag, concat_ws, count, sort_array, lit
from pyspark.sql.types import *
from pyspark.sql import DataFrame, functions as F, Window

class Analytics:

    def __init__(self, df: DataFrame):
        """
            Adds a timestamp column to the dataframe
            :param df: pandas dataframe
        """
        if df is None:
            raise ValueError("A DataFrame must be provided")
        
        # self.df = df.withColumn('timestamp', current_timestamp().cast(TimestampType())) \
        #     .withWatermark('timestamp', '5 seconds')   -> casting to Timestamp is error
        
        self.df = df.withColumn('timestamp', current_timestamp().cast(StringType())) \
                    .withWatermark('timestamp', '5 seconds')
    
    def avg_duration_per_page(self):
        """
        Calculate average duration on each page in seconds
        :param df: pandas dataframe
        :return: dataframe with average duration on each page in seconds
        """
        avg_duration_per_page = self.df \
            .groupBy(col('Page_URL'), "timestamp") \
            .agg(avg('Duration_on_Page_s').alias('Avg_Duration_on_Page'))

        # Convert duration from seconds to minutes and seconds
        avg_duration_per_page = avg_duration_per_page \
            .withColumn('Avg_Duration_minutes', floor(col('Avg_Duration_on_Page') / 60)) \
            .withColumn('Avg_Duration_seconds', col('Avg_Duration_on_Page') % 60) \
            .select('Page_URL', 'Avg_Duration_minutes', 'Avg_Duration_seconds')

        return avg_duration_per_page
    
    def count_sessions_per_country(self):
        """
        Count sessions per country
        :param df: pandas dataframe
        :return: dataframe with country and count of sessions
        """
        sessions_per_country = self.df \
            .groupBy(col("Country"), "timestamp") \
            .count() \
            .withColumnRenamed("count", "Session Count")

        return sessions_per_country
    
    def calculate_page_visit_counts(self):
        """
        Calculate page visit counts
        :param df: pandas dataframe
        :return: dataframe with page visit counts
        """
        page_visit_counts = self.df \
            .groupBy(col('Page_URL'), "timestamp") \
            .count()
            # .orderBy('count', ascending=False) \
            # .sort('count')
        
        
        return page_visit_counts
    
    def count_interaction_types(self):
        """
        Count interaction types
        :param df: pandas dataframe
        :return: dataframe with interaction type counts
        """
        interaction_counts = self.df \
            .groupBy(col('Interaction_Type'), "timestamp") \
            .count()
            # .orderBy('count', ascending=False)

        return interaction_counts
    
    def device_type_distribution(self):
        """
        Device type distribution
        :param df: pandas dataframe
        :return: dataframe with device type distribution
        """
        device_type_distribution = self.df \
            .groupBy(col('Device_Type'), "timestamp") \
            .count()
            # .orderBy('count', ascending=False)
        
        return device_type_distribution
    
    def page_views_by_country(self):
        """
        Page views by country
        :param df: pandas dataframe
        :return: dataframe with page views by country
        """
        page_views_by_country = self.df \
            .groupBy(col('Country'), "timestamp") \
            .count()
            # .orderBy('count', ascending=False)
        return page_views_by_country

    def identify_popular_paths_non_stream(self):
        """
        Identifies popular paths taken by users
        :return: dataframe with popular paths
        """
        window = Window.partitionBy("user_id").orderBy("Timestamp")

        # Collect all pages visited by a user in a session into a list
        paths_df = self.df.withColumn("Visited_Pages", sort_array(collect_list("Page_URL").over(window)))

        # Create paths from the list of visited pages
        paths_df = paths_df.withColumn("Path", concat_ws(" -> ", "Visited_Pages"))

        popular_paths_df = paths_df.groupBy("Path").agg(count("*").alias("Count")).orderBy("Count", ascending=False)

        return popular_paths_df