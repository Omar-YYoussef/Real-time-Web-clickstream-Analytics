from pyspark.sql.functions import min, max, avg, col, floor, countDistinct, desc

import pandas as pd
from DBConnection import DB

db = DB()
# Create a cursor
cursor = db.connection.cursor()

insert_query = "INSERT INTO DeviceTypeCounts (Device_Type, Count) VALUES (%s, %s)"
values_to_insert = ('laptop', '15550')
cursor.execute(insert_query, values_to_insert)

# Commit the transaction
db.connection.commit()


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
db.connection.close()