from pyspark.sql.functions import min, max, avg, col, floor, countDistinct, desc
import pandas as pd
import mysql.connector

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

# Create a cursor
cursor = connection.cursor()

insert_query = "INSERT INTO DeviceTypeCounts (Device_Type, Count) VALUES (%s, %s)"
values_to_insert = ('SAMSUN', '1550')
cursor.execute(insert_query, values_to_insert)

# Commit the transaction
connection.commit()


select_query = "SELECT * FROM DeviceTypeCounts"

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
connection.close()