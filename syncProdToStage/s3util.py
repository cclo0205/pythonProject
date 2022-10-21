from argparse import FileType
import sys
import json
import os
import requests
import time
from datetime import datetime
from elasticsearch import Elasticsearch
from optparse import OptionParser
import os
from urllib.parse import urlparse
import glob
import boto3
from botocore.config import Config
from botocore.exceptions import NoCredentialsError

# prod
PROD_ACCESS_KEY = ''
PROD_SECRET_KEY = ''

# stage 
ACCESS_KEY = ''
SECRET_KEY = ''

file_path_base = "file/"
class s3util:
    def phaseBinaryToObj(binary):
        if "mobileapp" in binary:
            index = binary.find("mobileapp/")
            
        else:
            index = binary.find("app/")

        if "?" in binary:
            end = binary.find("?")
            return binary[index:end]
        else:
            return binary[index:]

    def downloadBinaryFile(appId, appType, binary):
        print("downloadBinaryFile# appId:", appId, " #appType: ", appType)
        my_config = Config(
            region_name='us-east-2'
        )
        s3 = boto3.client('s3', config=my_config, aws_access_key_id=PROD_ACCESS_KEY, aws_secret_access_key=PROD_SECRET_KEY)
        bucket = "htc-vr-cms-submitted-prod-use2-20200406"
        fileType = ".apk"
        if appType == 0:
            fileType = ".zip"
        
        # objectName = s3Folder + appId +"/binary-draft/" + appId + "_beta" + fileType
        objectName = s3util.phaseBinaryToObj(binary)
        fileName = "file/" + appId + "_beta" + fileType
        print("downloadBinaryFile# objectName: ", objectName)
        try:
            s3.download_file(bucket, objectName, fileName)
            print("downloadBinaryFile# Download Successful")
            return fileName
        except FileNotFoundError:
            print("downloadBinaryFile# File Not Found")
            sys.exit(10)
        except NoCredentialsError:
            print("downloadBinaryFile# Fail")
            sys.exit(11)

    def downloadFile(url):
        print("downloadFile url: ", url)
        myfile = requests.get(url)
        file_info = s3util.getFileInfo(url)
        file_download_path = file_path_base + file_info['file_name']
        open(file_download_path, 'wb').write(myfile.content)
        return file_download_path

    def uploadFileToS3(local_file, bucket_name, s3_file_name):
        stage_s3_file_url = "https://assets-stage-usw2.viveport.com/vr_developer_published_assets/" + s3_file_name
        my_config = Config(
            region_name='us-west-2'
        )
        s3 = boto3.client('s3', config=my_config, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        try:
            s3.upload_file(local_file, bucket_name, s3_file_name)
            print("uploadFileToS3# Upload Successful\tbucket_name:", bucket_name, "\ts3_file_name:",s3_file_name)
            return stage_s3_file_url
        except FileNotFoundError:
            print("uploadFileToS3# File Not Found\tbucket_name:", bucket_name, "\ts3_file_name:",s3_file_name)
            sys.exit(10)
        except NoCredentialsError:
            print("uploadFileToS3# Fail, local_file\tbucket_name:", bucket_name, "\ts3_file_name:",s3_file_name)
            sys.exit(11)

    def getFileInfo(url):
        u = urlparse(url)
        s3_file_path = s3util.getS3FilePath(u.path)
        return {
            'path': u.path,
            'file_name': os.path.basename(u.path),
            's3_file_path': s3_file_path
        }

    def cleanFolder():
        print("cleanFolder")
        f_p = file_path_base + "*"
        files = glob.glob(f_p)
        for f in files:
            os.remove(f)
    
    def getS3FilePath(path):
        if "mobileapp" in path:
            index = path.find("mobileapp/")
        else:
            index = path.find("app/")
        return path[index:]