import os
import json
import shutil
import datetime
from datetime import datetime
from datetime import timedelta
import boto3
from boto3 import resource, client
import botocore


def lambda_handler(event, context):
        
    def initialize_objects_and_varibales():
        global list_efs_files_zip
        global list_efs_files_unzip
        global SOURCE_BUCKET_NAME
        global FILE_NAME_ZIP
        global FILE_NAME_UNZIP
        global FILE_NAME_WITH_DIRECTORY_ZIP
        global FILE_NAME_WITH_DIRECTORY_UNZIP
        global dt
        global File_PREFIX_DATE
        global FILE_PREFIX
        
        dt = datetime.now()
        File_PREFIX_DATE = dt.strftime('%d%m%Y')
        FILE_PREFIX_DIRECTORY = os.getenv("bucket_sub_directory")
        FILE_SUFFIX_ZIP = os.getenv("file_suffix_zip")
        FILE_SUFFIX_UNZIP = os.getenv("file_suffix_unzip")
        SOURCE_BUCKET_NAME = os.getenv("bucket_name")
        FILE_TYPE = os.getenv('fileType')
        FILE_PREFIX = os.getenv("file_prefix")
        
        incoming_path_zip = os.environ['incoming_path_zip']
        list_efs_files_zip = os.listdir(os.environ['incoming_path_zip'])
        list_efs_files_zip.sort()
        
        incoming_path_unzip = os.environ['incoming_path_unzip']
        list_efs_files_unzip = os.listdir(os.environ['incoming_path_unzip'])
        list_efs_files_unzip.sort()
        
    
        print("---------------------------------")
        print("Files in EFS zip folder are: ", list_efs_files_zip)
        print("Files in EFS unzip folder are: ", list_efs_files_unzip)
        
        
        if FILE_PREFIX_DIRECTORY == 'False':
            
            if FILE_TYPE == 'daily':
                FILE_NAME_ZIP = FILE_PREFIX+File_PREFIX_DATE+FILE_SUFFIX_ZIP
                FILE_NAME_WITH_DIRECTORY_ZIP = FILE_NAME_ZIP
                FILE_NAME_UNZIP = FILE_PREFIX+File_PREFIX_DATE+FILE_SUFFIX_UNZIP
                FILE_NAME_WITH_DIRECTORY_UNZIP = FILE_NAME_UNZIP
            else:
                FILE_NAME_ZIP = FILE_SUFFIX_ZIP
                FILE_NAME_WITH_DIRECTORY_ZIP = FILE_NAME_ZIP   
                FILE_NAME_UNZIP = FILE_SUFFIX_UNZIP
                FILE_NAME_WITH_DIRECTORY_UNZIP = FILE_NAME_UNZIP
                
        else:
            if FILE_TYPE == 'daily':
                FILE_NAME_ZIP = FILE_PREFIX+File_PREFIX_DATE+FILE_SUFFIX_ZIP
                FILE_NAME_WITH_DIRECTORY_ZIP = FILE_PREFIX_DIRECTORY+FILE_NAME_ZIP
                FILE_NAME_UNZIP = FILE_PREFIX+File_PREFIX_DATE+FILE_SUFFIX_UNZIP
                FILE_NAME_WITH_DIRECTORY_UNZIP = FILE_PREFIX_DIRECTORY+FILE_NAME_UNZIP
            else:
                FILE_NAME_ZIP = FILE_SUFFIX_ZIP
                FILE_NAME_WITH_DIRECTORY_ZIP = FILE_PREFIX_DIRECTORY+FILE_NAME_ZIP
                FILE_NAME_UNZIP = FILE_SUFFIX_UNZIP
                FILE_NAME_WITH_DIRECTORY_UNZIP = FILE_PREFIX_DIRECTORY+FILE_NAME_UNZIP
                
        print("Files we are looking for in EFS zip folder is: ", FILE_NAME_WITH_DIRECTORY_ZIP)
        print("Files we are looking for in EFS unzip folder is: ", FILE_NAME_WITH_DIRECTORY_UNZIP)

    
    def check_file_existance():
        
        s3 = resource('s3')
        
            
        #if FILE_NAME_WITH_DIRECTORY_ZIP in list_efs_files_zip:
        #    print("[SUCCESS]", dt, "File Found in EFS zip folder:", FILE_NAME_WITH_DIRECTORY_ZIP)
        #else:
        #    print("[ERROR]", dt, "File not found in EFS zip folder:", FILE_NAME_WITH_DIRECTORY_ZIP)
            
        
        #if FILE_NAME_WITH_DIRECTORY_UNZIP in list_efs_files_unzip:
        #    print("[SUCCESS]", dt, "File Found in EFS unzip folder:", FILE_NAME_WITH_DIRECTORY_UNZIP)
        #else:
        #    print("[ERROR]", dt, "File not found in EFS unzip folder:", FILE_NAME_WITH_DIRECTORY_UNZIP)
        
        #try:
        if FILE_NAME_WITH_DIRECTORY_ZIP in list_efs_files_zip:
            if FILE_NAME_WITH_DIRECTORY_UNZIP in list_efs_files_unzip:
                print("[SUCCESS]", dt, "File Found in EFS unzip folder:", FILE_NAME_WITH_DIRECTORY_UNZIP)
            print("[SUCCESS]", dt, "File Found in EFS zip folder:", FILE_NAME_WITH_DIRECTORY_ZIP) 
                
        else:
            print("[ERROR]", dt, "File not found in EFS zip and unzip folder:", FILE_NAME_WITH_DIRECTORY_ZIP, FILE_NAME_WITH_DIRECTORY_UNZIP)
            print("---------------------------------") 
            raise SystemExit   
            
        #except Exception as e:
        #    print("[ERROR]", dt, "File not found in EFS zip and unzip folder:", FILE_NAME_WITH_DIRECTORY_ZIP)
        #    raise SystemExit(e)
            
            
                
        print("---------------------------------")    
        
    initialize_objects_and_varibales()
    check_file_existance() 
    
    return {
        'statusCode': 200,
        'body': json.dumps('Event Watcher Function for EFS is successful !')
    }
