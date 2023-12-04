"""
    Calculate session duration from start and end times.
"""
def calculate_session_duration(start_time, end_time):
    return (end_time - start_time).seconds
