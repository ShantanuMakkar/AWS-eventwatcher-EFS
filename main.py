import os
import json
import shutil
import datetime
from datetime import datetime
from datetime import timedelta
import boto3


def lambda_handler(event, context):
    
    '''
    
    file_prefix = os.environ['file_prefix']
    data_file_extention = os.environ['data_file_extention']
    processing_dir = os.environ['processing_dir']
    new_file_arrival_time=os.environ['new_file_arrival_time']
    #incoming_path = os.environ['incoming_path']
    
    incoming_path_unzip = os.environ['incoming_path_unzip']
    incoming_path_zip = os.environ['incoming_path_zip']

    customer_start_exec = os.environ['customer_start_exec']
    cust_start_time = datetime.strptime(customer_start_exec,'%Y-%m-%dT%H:%M:%S.%fZ')
    
    today = datetime.now()
    yesterday = datetime.now() - timedelta(days=1)
    month = today.strftime("%m")
    year = today.strftime("%Y")
    date = today.strftime("%d")
    today_date = today.strftime("%Y%m%d")
    previous_date = yesterday.strftime("%Y%m%d")
    
    new_file_arrival_hour = int(new_file_arrival_time.split(':')[0])
    new_file_arrival_minutes = int(new_file_arrival_time.split(':')[1])
    new_file_arrival_day = today.replace(hour=new_file_arrival_hour, minute=new_file_arrival_minutes, second=0, microsecond=0)
    

    if (today.day == cust_start_time.day) and (today >= new_file_arrival_day):
        Customer = file_prefix + today_date
    else:
        Customer = file_prefix + previous_date
   
    processing_path = "/mnt" + processing_dir
    path_year = processing_path + year
    path_month = path_year + "/" + month
    path_date = path_month + "/" + date
    datafile = path_date + "/datafile/"

    if not os.path.exists(processing_path):
        os.mkdir(processing_path)
    if not os.path.exists(path_year):
        os.mkdir(path_year)
    if not os.path.exists(path_month):
        os.mkdir(path_month)
    if not os.path.exists(path_date):
        os.mkdir(path_date)
    if not os.path.exists(datafile):
        os.mkdir(datafile)
    
    CustomerFile = ""
    
    #list_efs_files = os.listdir(os.environ['incoming_path'])
    #list_efs_files.sort()
    
    #list_efs_files_unzip = os.listdir(os.environ['incoming_path_unzip'])
    #list_efs_files_unzip.sort()
    
    list_efs_files_zip = os.listdir(os.environ['incoming_path_zip'])
    list_efs_files_zip.sort()


    #for custfile in list_efs_files:
    for custfile in list_efs_files_zip:
        if custfile.find(Customer) != -1:
            if custfile.find(data_file_extention) != -1:
                CustomerFile = custfile
                shutil.copy(incoming_path + custfile, processing_path +
                            year + "/" + month + "/" + date + "/datafile/" + custfile)
                break
    
    filetype = ""
    #for file in list_efs_files:
    for file in list_efs_files_zip:
        if file.find(Customer) != -1:
            if file.find("txt") != -1:
                filetype = "txt"
            else:
                filetype = "txt"

    if filetype != "":
        if filetype == "txt":
            CustomerFilewithoutgpg = CustomerFile.strip(".txt")
        else:
            CustomerFilewithoutgpg = CustomerFile.strip(".txt")
            
            
    if(len(CustomerFile) != 0):
        return {
            'Customer': processing_dir + year + "/" + month + "/" + date + "/datafile/" + CustomerFile
        }
        
    else:
        return{
            'FilesNotAvailable': "Files not available in incoming folder",
            'ExpectedDataFile': Customer + "xxxxxx.txt",
            #'FilesIncoming': list_efs_files,
            #'FilesIncoming-unzip': list_efs_files_unzip,
            'FilesIncoming-zip': list_efs_files_zip
            }
            
    '''

import boto3
import os
from boto3 import resource, client
import botocore
from datetime import datetime


def lambda_handler(event, context):
        
    def initialize_objects_and_varibales():
        global list_efs_files_zip
        global SOURCE_BUCKET_NAME
        global FILE_NAME
        global FILE_NAME_WITH_DIRECTORY
        global dt
        global File_PREFIX_DATE
        
        dt = datetime.now()
        File_PREFIX_DATE = dt.strftime('%Y%m%d')
        FILE_PREFIX_DIRECTORY = os.getenv("bucket_sub_directory")
        FILE_SUFFIX = os.getenv("file_suffix")
        SOURCE_BUCKET_NAME = os.getenv("bucket_name")
        FILE_TYPE = os.getenv('fileType')
        
        incoming_path_zip = os.environ['incoming_path_zip']
        list_efs_files_zip = os.listdir(os.environ['incoming_path_zip'])
        list_efs_files_zip.sort()
        
        print("---------------------------------")
        print("Files in EFS zip folder are: ", list_efs_files_zip)
        
        
        if FILE_PREFIX_DIRECTORY == 'False':
            
            if FILE_TYPE == 'daily':
                FILE_NAME = File_PREFIX_DATE+FILE_SUFFIX
                FILE_NAME_WITH_DIRECTORY = FILE_NAME
            else:
                FILE_NAME = FILE_SUFFIX
                FILE_NAME_WITH_DIRECTORY = FILE_NAME   
                
        else:
            if FILE_TYPE == 'daily':
                FILE_NAME = File_PREFIX_DATE+FILE_SUFFIX
                FILE_NAME_WITH_DIRECTORY = FILE_PREFIX_DIRECTORY+FILE_NAME    
            else:
                FILE_NAME = FILE_SUFFIX
                FILE_NAME_WITH_DIRECTORY = FILE_PREFIX_DIRECTORY+FILE_NAME
                
        print("Files we are looking for in EFS is: ", FILE_NAME_WITH_DIRECTORY)

    
    def check_file_existance():
        
        s3 = resource('s3')
        
            
        if FILE_NAME_WITH_DIRECTORY in list_efs_files_zip:
            print("[SUCCESS]", dt, "File Found in EFS Zip folder:", FILE_NAME_WITH_DIRECTORY)
        else:
            print("[ERROR]", dt, "File not found in EFS Zip folder:", FILE_NAME_WITH_DIRECTORY)
    
                
        print("---------------------------------")    
        
    initialize_objects_and_varibales()
    check_file_existance() 
