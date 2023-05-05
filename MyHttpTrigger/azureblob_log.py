from azure.storage.blob import BlobServiceClient

# Initialize a BlobServiceClient object
connection_string = "DefaultEndpointsProtocol=https;AccountName=rgazurefunctionleara88b;AccountKey=OVOmqp+ff5lWSwnw8L8MFT0xyfMohHCiIx2ON2X8vx3cX81ocMg7jYQko0i45vmfjKfpWaK+beQ6+ASt+aQ3fg==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)


# Set the container name and blob path for the analytics logs
container_name = "demo"
blob_path = "database_1/schema/table_a/year=2023/month=12/day=31/table_a_20231231_245959.txt"

# Get a reference to the analytics blob
blob_client = blob_service_client.get_blob_client(container_name, blob_path)

# Download the blob contents as a string
blob_contents = blob_client.download_blob().readall()

# Print the contents of the analytics blob
print(blob_contents)


# # # Path: MyFunctionProj\MyHttpTrigger\azureblob_log.py

# from azure.storage.blob import BlobServiceClient

# # Initialize a BlobServiceClient object
# # connection_string = "<your_connection_string>"
# blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# # List the containers in the storage account
# container_list = blob_service_client.list_containers()

# # Print the name of each container
# for container in container_list:
#     print(container.name)

# # Set the container name
# container_name = "demo"

# # Get a reference to the container
# container_client = blob_service_client.get_container_client(container_name)

# # List all blobs in the container recursively
# blobs = container_client.list_blobs()

# # Print the name of each blob
# for blob in blobs:
#     print(blob.name)






from azure.storage.blob import BlobServiceClient

# Get a reference to the blob
blob_client = blob_service_client.get_blob_client(container_name, blob_path)

# Get the current metadata for the blob
metadata = blob_client.get_blob_properties().metadata

# print the current metadata
print(metadata)

# Add or update the metadata to include the last access time and user information
metadata['last_access_time'] = '2020-01-15T12:00:00Z'
metadata['user'] = 'user1'
blob_client.set_blob_metadata(metadata=metadata)

metadata_after = blob_client.get_blob_properties().metadata
print(metadata_after)
