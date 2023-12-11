from pyspark.sql.functions import window, avg, col, floor, current_timestamp, pandas_udf, collect_list, lag, concat_ws, count, sort_array, lit
from pyspark.sql.types import *
from pyspark.sql import DataFrame, functions as F, Window

class Analytics:

    @staticmethod
    def avg_duration_per_page(df):
        """
        Calculate average duration on each page in seconds
        :param df: pandas dataframe
        :return: dataframe with average duration on each page in seconds
        """
        avg_duration_per_page = df \
            .groupBy(col('Page_URL')) \
            .agg(avg('Duration_on_Page_s').alias('Avg_Duration_on_Page'))

        # Convert duration from seconds to minutes and seconds
        avg_duration_per_page = avg_duration_per_page \
            .withColumn('Avg_Duration_minutes', floor(col('Avg_Duration_on_Page') / 60)) \
            .withColumn('Avg_Duration_seconds', col('Avg_Duration_on_Page') % 60) \
            .select('Page_URL', 'Avg_Duration_minutes', 'Avg_Duration_seconds')

        return avg_duration_per_page, ['pageUrl', 'avgDurationMinutes', 'avgDurationSeconds'], 'avg_duration_per_page'
    
    @staticmethod
    def count_sessions_per_country(df):
        """
        Count sessions per country
        :param df: pandas dataframe
        :return: dataframe with country and count of sessions
        """
        sessions_per_country = df \
            .groupBy(col("Country")) \
            .count() \
            .orderBy('count', ascending=False)

        return sessions_per_country, ['country', 'sessionCount'], 'count_sessions_per_country'

    @staticmethod
    def calculate_page_visit_counts(df):
        """
        Calculate page visit counts
        :param df: pandas dataframe
        :return: dataframe with page visit counts
        """        
        page_visit_counts = df \
            .groupBy(col('Page_URL')) \
            .count() \
            .withColumnRenamed("count", "pageUrlCount") \
            # .orderBy('count', ascending=False)

        return page_visit_counts, ['pageUrl', 'pageUrlCount'], 'page_visit_counts'
    
    @staticmethod
    def count_interaction_types(df):
        """
        Count interaction types
        :param df: pandas dataframe
        :return: dataframe with interaction type counts
        """
        interaction_counts = df \
            .groupBy(col('Interaction_Type')) \
            .count() \
            .orderBy('count', ascending=False)

        return interaction_counts, ['interactionType', 'interactionTypeCount'], 'count_interaction_types'
    
    @staticmethod
    def device_type_distribution(df):
        """
        Device type distribution
        :param df: pandas dataframe
        :return: dataframe with device type distribution
        """
        device_type_distribution = df \
            .groupBy(col('Device_Type')) \
            .count() \
            .orderBy('count', ascending=False)
        
        return device_type_distribution, ['deviceType', 'deviceTypeCount'], 'device_type_distribution'
    
    @staticmethod
    def page_views_by_country(df):
        """
        Page views by country
        :param df: pandas dataframe
        :return: dataframe with page views by country
        """
        page_views_by_country = df \
            .groupBy(col('Country')) \
            .count() \
            .orderBy('count', ascending=False)

        return page_views_by_country, ['country', 'countryCount'], 'page_views_by_country'

    @staticmethod
    def identify_popular_paths_non_stream(df):
        """
        Identifies popular paths taken by users
        :return: dataframe with popular paths
        """
        window = Window.partitionBy("user_id").orderBy("Timestamp")

        # Collect all pages visited by a user in a session into a list
        paths_df = df.withColumn("Visited_Pages", sort_array(collect_list("Page_URL").over(window)))

        # Create paths from the list of visited pages
        paths_df = paths_df.withColumn("Path", concat_ws(" -> ", "Visited_Pages"))

        popular_paths_df = paths_df.groupBy("Path").agg(count("*").alias("Count")).orderBy("Count", ascending=False)

        return popular_paths_df, ['path', 'pathCount'], 'popular_paths'
    