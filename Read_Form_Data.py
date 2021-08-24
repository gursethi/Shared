# Import LIbararies

!pip install azure-ai-formrecognizer --pre
!pip install azure-storage-blob


# Import Function

from azure.core.exceptions import ResourceNotFoundError
from azure.ai.formrecognizer import FormRecognizerClient
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import os
import sys
from os import path
from requests import get, post

# Create Form Recognizer Client Connection

endpoint = "ENDPOINT FROM FORM RECOGNIZER SERVICE"
key = "KEY FROM FORM RECOGNIZER SERVICE"

form_recognizer_client = FormRecognizerClient(endpoint, AzureKeyCredential(key))

# Connection To Blob Storage, Create RAW & PROCESSED Client Connection
# Create the BlobServiceClient object which will be used to get the container_client
connect_str = "Access Key From Azure Storage Account"
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# Container client for raw container.
raw_container_client = blob_service_client.get_container_client("raw")

# Container client for processed container
processed_container_client = blob_service_client.get_container_client("processed")

# Get base url for container.
invoiceUrlBase = raw_container_client.primary_endpoint
csvUrlBase = processed_container_client.primary_endpoint


# Read Input Files using Form Recognizer SERVICE

print("\nProcessing blobs...")

blob_list = raw_container_client.list_blobs()

for blob in blob_list:
    invoiceUrl = f'{invoiceUrlBase}/{blob.name}'
    print(invoiceUrl)
    poller = form_recognizer_client.begin_recognize_receipts_from_url(invoiceUrl)

    # Get resultsimport json
    from requests import post
    invoices = poller.result()

# Save output To a File in Processed Folder

# Create a local directory to hold blob data
local_path = "./data"
if path.exists(local_path):
  pass
else:
  os.mkdir(local_path)


# Create a file in the local data directory to upload and it to Process Folder

# Replace file extension from incoming file and replace it with .csv
local_file_name = blob.name.split('.')[0]+'.csv'
upload_file_path = os.path.join(local_path, local_file_name)

# Open Local File
restorePoint = sys.stdout
sys.stdout = open(upload_file_path, 'w')

# Traverse thru incoming data to Print Header and Items information

for receipt in invoices:
    for name, field in receipt.fields.items():
        if name == "Items":
            #x.append({"Receipt File Name",blob.name})
            print("File Name:",blob.name)
            for idx, items in enumerate(field.value):
                #print("Item:{}".format(idx+1))
                for item_name, item in items.value.items():
                    #x.append({item_name:item.value})
                    print("{}: {}".format(item_name, item.value))
        else:
          #x.append({name:field.value})
          print("{}: {}".format(name, field.value))
sys.stdout.close()
sys.stdout = restorePoint
file.close()


#Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(container="processed", blob=local_file_name)

#print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

# Upload the created file
with open(upload_file_path, "rb") as data:
  blob_client.upload_blob(data)