import json
import logging
import os

import azure.functions as func
from azure.storage.blob import BlobServiceClient, generate_blob_sas, AccessPolicy, BlobSasPermissions, BlobLeaseClient
from azure.core.exceptions import ResourceExistsError
from datetime import datetime, timedelta
import requests
from adal import AuthenticationContext
import time
from azure.identity import ManagedIdentityCredential

def main(event: func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('Python EventGrid trigger processed an event: %s', result)

    blob_url = event.get_json().get('url')
    logging.info('blob URL: %s', blob_url)
    blob_name = blob_url.split("/")[-1].split("?")[0]
    logging.info('blob name: %s', blob_name)
    origin_container_name = blob_url.split("/")[-2].split("?")[0]
    logging.info('container name: %s', origin_container_name)

#Change below parameters, or feed them via Orchestrator

    target_container_name= "azureml-blobstore-be99d911-97df-4c7a-843d-fc7c5c3f58a3"
    target_base_path="mnist/landing/"
   

    def copy_blob(blob_url,source_client,target_client,target_container_name,blob_name):
        processed_container = target_client.get_container_client(target_container_name)
    # Create new Container if not exist
        try:
            processed_container.create_container()
        except ResourceExistsError:
            pass
    #Copy blob:
        try:
            new_blob = target_client.get_blob_client(target_container_name, target_base_path+blob_name) 
            blob_to_copy=source_client.get_blob_client(origin_container_name, blob_name)
          
            new_blob.start_copy_from_url(blob_to_copy.primary_endpoint)#+'?'+sas_token)
        except:
            logging.info('Copying blob not succesful')

    def delete_processed_blob(client,container,blob_name):
        try:
            blob_to_delete = client.get_blob_client(container=container, blob=blob_name)
            blob_to_delete.delete_blob(delete_snapshots=False)

        except:
            logging.info("blob not deleted")


# The default credential first checks environment variables for configuration as described above.
# If environment configuration is incomplete, it will try managed identity.
    credential = ManagedIdentityCredential()
    # Connect to source blob account
    blob_service_client_origin = BlobServiceClient(account_url="https://amlpocwsstorage.blob.core.windows.net/",credential=credential)

    #Connect to target blob account 
    blob_service_client_target= BlobServiceClient(account_url="https://amlpocwsstorage.blob.core.windows.net/",credential=credential)
    #connect to specific original container
    container_client=blob_service_client_origin.get_container_client(container=origin_container_name)

    #Extract_filenames in blob
    arr_files=[x['name'] for x in container_client.list_blobs()]
    #Check which file type arrived, and if the corresponding other file type is present, if so execute following:
    #1) get blob_url
    #2) Copy both blobs
    #3) Delete both blos
    #4) basic logging added
    if os.path.splitext(blob_name)[1] ==".png":
        if(os.path.splitext(blob_name)[0]+".txt") in arr_files:
            logging.info("both files present, trigger pipeline")
            blob_url2=blob_service_client_origin.get_blob_client(origin_container_name, str(os.path.splitext(blob_name)[0])+".txt").url
            copy_blob(blob_url,blob_service_client_origin,blob_service_client_target,target_container_name,blob_name)
            copy_blob(blob_url2,blob_service_client_origin,blob_service_client_target,target_container_name,str(os.path.splitext(blob_name)[0])+".txt")
            logging.info("files copied succesfully")
            delete_processed_blob(blob_service_client_origin,origin_container_name,blob_name)
            delete_processed_blob(blob_service_client_origin,origin_container_name,str(os.path.splitext(blob_name)[0])+".txt")
            logging.info("files deleted in raw")

        else:
            logging.info("txt file not yet arrived")
    elif os.path.splitext(blob_name)[1] ==".txt":
        if(os.path.splitext(blob_name)[0]+".png") in arr_files:
            logging.info("both files present, trigger pipeline")
            blob_url2=blob_service_client_origin.get_blob_client(origin_container_name, str(os.path.splitext(blob_name)[0])+".png").url
            copy_blob(blob_url,blob_service_client_origin,blob_service_client_target,target_container_name,blob_name)
            copy_blob(blob_url2,blob_service_client_origin,blob_service_client_target,target_container_name,str(os.path.splitext(blob_name)[0])+".png")
            logging.info("files copied succesfully")
            delete_processed_blob(blob_service_client_origin,origin_container_name,blob_name)
            delete_processed_blob(blob_service_client_origin,origin_container_name,str(os.path.splitext(blob_name)[0])+".png")
            logging.info("files deleted in raw")

        else:
            logging.info("png file not yet arrived")
    else:
        logging.info("not in expected format")
