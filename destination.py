import json
import snowflake.connector
import pandas as pd


with open("config.json") as json_data_file:
    credentials = json.load(json_data_file)

# Define Snowflake connection parameters
sf_creds = credentials['Snowflake']
connection = snowflake.connector.connect(
    user=sf_creds['user'],
    password=sf_creds['password'],
    account=sf_creds['account'],
    warehouse=sf_creds['warehouse'],
    database=sf_creds['database'],
    schema=sf_creds['schema']
)

print("Connection successful!")

query = "SELECT * FROM CARS;"
df = pd.read_sql(query, connection)

# Close the Snowflake connection
connection.close()

def get_destination_data():
    return df
