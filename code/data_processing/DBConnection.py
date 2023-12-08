import pandas as pd
import mysql.connector
class DB :
    # Replace these values with your actual database connection details
    db_user = 'bigdata'
    db_pass = 'hytham123'
    host = 'p3nlmysql47plsk.secureserver.net'
    port = 3306
    database_name = 'Clickstream_DB'

    # Establish a connection
    connection = mysql.connector.connect(
        user=db_user,
        password=db_pass,
        host=host,
        port=port,
        database=database_name
    )
