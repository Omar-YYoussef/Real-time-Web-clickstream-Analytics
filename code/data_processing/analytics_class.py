from pyspark.sql.functions import min, max, avg, col, floor, countDistinct, desc


class Analytics:

    def __init__(self):
        pass

    @staticmethod
    def avg_duration_per_page(df):
        """
        Calculate average duration on each page in seconds
        :param df: pandas dataframe
        :return: dataframe with average duration on each page in seconds
        """
        avg_duration_per_page = df \
            .groupBy('Page_URL') \
            .agg(avg('Duration_on_Page_s').alias('Avg_Duration_on_Page'))

        # Convert duration from seconds to minutes and seconds
        avg_duration_per_page = avg_duration_per_page \
            .withColumn('Avg_Duration_minutes', floor(col('Avg_Duration_on_Page') / 60)) \
            .withColumn('Avg_Duration_seconds', col('Avg_Duration_on_Page') % 60) \
            .select('Page_URL', 'Avg_Duration_minutes', 'Avg_Duration_seconds')

        return avg_duration_per_page
    
    @staticmethod
    def count_sessions_per_country(df):
        """
        Count sessions per country
        :param df: pandas dataframe
        :return: dataframe with country and count of sessions
        """
        sessions_per_country = df \
            .groupBy("Country") \
            .count() \
            .withColumnRenamed("count", "Session Count")

        return sessions_per_country
    
    @staticmethod
    def calculate_page_visit_counts(df):
        """
        Calculate page visit counts
        :param df: pandas dataframe
        :return: dataframe with page visit counts
        """
        page_visit_counts = df \
            .groupBy('Page_URL') \
            .count() \
            .orderBy('count', ascending=False) \
            .sort('count')
        

        return page_visit_counts

    @staticmethod
    def count_interaction_types(df):
        """
        Count interaction types
        :param df: pandas dataframe
        :return: dataframe with interaction type counts
        """
        interaction_counts = df \
            .groupBy('Interaction_Type') \
            .count() \
            .orderBy('count', ascending=False)

        return interaction_counts
    
    @staticmethod
    def device_type_distribution(df):
        """
        Device type distribution
        :param df: pandas dataframe
        :return: dataframe with device type distribution
        """
        device_type_distribution = df \
            .groupBy('Device_Type') \
            .count() \
            .orderBy('count', ascending=False)

        return device_type_distribution
    
    @staticmethod
    def page_views_by_country(df):
        """
        Page views by country
        :param df: pandas dataframe
        :return: dataframe with page views by country
        """
        page_views_by_country = df \
            .groupBy('Country') \
            .count() \
            .orderBy('count', ascending=False)
        
        return page_views_by_country
    

