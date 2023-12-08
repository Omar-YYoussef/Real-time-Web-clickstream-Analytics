import pandas as pd
import mysql.connector
import numpy as np

# Replace these values with your actual database connection details
db_user = 'bigdata'
db_pass = 'hytham123'
host = 'p3nlmysql47plsk.secureserver.net'
port = 3306
database_name = 'Clickstream_DB'

# Establish a connection
conn = mysql.connector.connect(
    user=db_user,
    password=db_pass,
    host=host,
    port=port,
    database=database_name
)

# Create a cursor
cursor = conn.cursor()

data_to_insert = np.array([
    ['uik', 2],
    ['ll', 4],
    ['op', 6]
])
for data in data_to_insert:
        cursor.execute("INSERT INTO Page_URLCounts (Page_URL, Count) VALUES (%s, %s)", tuple(data))
        # Commit the transaction
        conn.commit()
#Commit the transaction
conn.commit()