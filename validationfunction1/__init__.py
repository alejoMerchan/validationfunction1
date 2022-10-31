import logging
from operator import contains
from pydoc import getpager
from tokenize import String

import azure.functions as func
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, generate_account_sas, ResourceTypes, AccountSasPermissions
import csv


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("--- starting the function ---")
    try:
        req_body = req.get_json()
    
        connection = get_parameter(req_body,"token")
        blob_svc = get_blob_sercie_client(connection)
    
        raw_container_name = get_parameter(req_body, "container_source")    
        container_client = blob_svc.get_container_client(raw_container_name)
    
        raw_data_name = get_parameter(req_body, "raw_file_name")
        blob_client = container_client.get_blob_client(raw_data_name)
    
        '''
        This is an example about how to get the blob content:
    
        blob_download = blob_client.download_blob()
        blob_content = blob_download.readall().decode("utf-8")
        print(f"type: {type(blob_content)}")
        print(f"Your content is: '{blob_content}'")
    
        '''
    
        temp_input_blob = get_parameter(req_body, "tmp_in_file")
        temp_output_blob = get_parameter(req_body, "tmp_out_file")
    
        with open(temp_input_blob, "wb") as my_blob:
            download_stream = blob_client.download_blob()
            my_blob.write(download_stream.readall())

        with open(temp_input_blob, "r") as file, open(temp_output_blob, "w") as outFile:
            reader = csv.reader(file, delimiter=',')
            writer = csv.writer(outFile, delimiter=',')
            header = next(reader)
            header += 'flag'
            writer.writerow(header)
            for row in reader:
                colValues = []
                for col in row:
                    colValues.append(col.lower())
                if(int(colValues[1]) > 30):
                    colValues.append('1')
                else:
                    colValues.append('2')

                writer.writerow(colValues)
    
        output_storage = get_parameter(req_body, "container_final")
        container_client = blob_svc.get_container_client(output_storage)
    
        dt_obj = datetime.now()
        date_to_str = dt_obj.strftime("%A %d-%b-%Y %H:%M:%S")
        with open(temp_output_blob, "rb") as blob_file:
            container_client.upload_blob(name=f"data-updated{date_to_str}.csv", data=blob_file)
    
        return func.HttpResponse(
            "The function was executed correctly",
            status_code=200
        )

    except:
        return func.HttpResponse(
             "Problem with the function",
             status_code=500
        )
        
def get_blob_sercie_client(conn_string: String) -> BlobServiceClient:
    try:
        blob = BlobServiceClient.from_connection_string(conn_str=conn_string)
        return blob
    except:
        return func.HttpResponse(
             "Problem creating the blob service client",
             status_code=500
        )
        
def get_parameter(request:String, key:String) -> String:
    try:
        parameter = request.get(key)
        return parameter
    except:
        return func.HttpResponse(
             "Problem getting the key from the json body",
             status_code=500
        )