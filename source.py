from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd
import json



with open("config.json") as json_data_file:
    credentials = json.load(json_data_file)

# Access Azure credentials
azure_creds = credentials['Azure']
STORAGE_ACCOUNT_NAME = azure_creds['storage_account']
STORAGE_ACCOUNT_KEY = azure_creds['access_key']
CONTAINER_NAME = azure_creds['container_name']



# Create a connection string
connection_string = (
    f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"
)

# Initialize the Blob Service Client
blob_service = BlobServiceClient.from_connection_string(connection_string)

# Access the container
container_client = blob_service.get_container_client(CONTAINER_NAME)

# Function to list all blob names in the container
def list_blob_names(container_client):
    return [blob.name for blob in container_client.list_blobs()]

# Function to generate a SAS URL for a blob
def generate_sas_url(account_name, container_name, blob_name, account_key, expiry_hours=1):
    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=expiry_hours),
    )
    return f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"


blob_names = list_blob_names(container_client)

# Load blobs into a list of dataframes
def load_parquet_files(blob_names, account_name, container_name, account_key):
    dataframes = []
    for blob_name in blob_names:
        sas_url = generate_sas_url(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=account_key,
        )
        df = pd.read_parquet(sas_url)
        dataframes.append(df)
    return dataframes

# Combine dataframes into a single dataframe
dataframes = load_parquet_files(blob_names, STORAGE_ACCOUNT_NAME, CONTAINER_NAME, STORAGE_ACCOUNT_KEY)
df = pd.concat(dataframes, ignore_index=True)

# Optionally: Display or save the combined dataframe
def get_source_data():
    return df
