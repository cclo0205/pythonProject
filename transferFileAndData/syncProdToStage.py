import sys
import json
import os
import requests
import time
from datetime import datetime
from elasticsearch import Elasticsearch
from optparse import OptionParser
import uuid
from esutil import esutil
from cmsutil import cmsutil
from s3util import s3util

error_list = []
admin_auth_key = ""
authorData = []

def get_prod_app_id_list():
    source = set()
    with open("app_list_prod.txt", "r") as f:
        for l in f.readlines():
            source.add(l.strip())
    prod_app_id_list = list(source)
    
    return prod_app_id_list

def sync_author_data_to_stage(prod_author_id):
    '''
    1. get prod author info
    2. save to stage author db
    '''
    print("| sync_author_data_to_stage start")
    
    for a in authorData:
        if a['_source']['id'] == prod_author_id:
            esutil.addStageAuthor(a['_source'])

    # prod_author_info = esutil.getProdAuthor(prod_author_id)
    # prod_author_data = prod_author_info['_source']
    # esutil.addStageAuthor(prod_author_data)

def sync_app_data_to_stage(prod_app_info):
    '''
    2.2 change file 
    2.2.1 download image/video/binary from prod 
    2.2.2 rename files name -> change appId 
    2.2.3 upload files to stage s3
    2.2.3 set stage files path to meta data
    '''
    print("| sync_app_data_to_stage start")
    
    '''
    upload cloud/binary/thumbnails/gallery
    '''
    #for test
    esutil.delStageApp(prod_app_info['id'], epoch_status = 1)
    esutil.delStageApp(prod_app_info['id'], epoch_status = 0)

    prod_app_info['thumbnails'] = cmsutil.syncThumbnailsFile(prod_app_info['thumbnails'])
    if 'cloud' in prod_app_info:
        prod_app_info['cloud'] = cmsutil.syncCloudFile(prod_app_info['cloud'])
    prod_app_info['gallery'] = cmsutil.syncGalleryFile(prod_app_info['gallery'])
    prod_app_info['binary'] = cmsutil.syncBinaryFile(prod_app_info['id'], prod_app_info['binary'], prod_app_info['app_type'])
    s3util.cleanFolder()
    
    #addStageApp epoch_status = 1
    esutil.addStageApp(prod_app_info) 

    #addStageApp epoch_status = 0
    prod_app_info_draft = prod_app_info
    prod_app_info_draft['epoch_status'] = 0
    prod_app_info['version_code'] = prod_app_info['version_code'] + 1 
    esutil.addStageApp(prod_app_info_draft) 

def sync_app_history_data_to_stage(prod_app_id):
    '''
    1. get prod history data
    2.0 get stage history data
    2.1 save data from prod to stage 
    '''

    # get prod 1 history
    # history_info = esutil.getProdHistory(prod_app_id)[0]['_source']
    # print(history_info)
    history_info = {
        "status": 0,
        "creator_name": "WADE CHEN",
        "version_code": 0,
        "app_id": prod_app_id,
        "creator_id": "Developer",
        "create_ts": 1525447115174,
        "message": "AppUpdate",
        "type": "LEGACY"
    }
    esutil.addStageHistory(history_info)

def process_wrapper(prod_app_id):
    url = "https://mgmt-stage-usw2.htcvive.com/api/cms-admin/v3/app/pstatus/" + prod_app_id
    
    payload = {
        "process_status": "3",
        "app_type": 1, 
        "reviewer_name": "Wade Chen"
    }

    headers = {
        'content-type': 'application/json',
        'authkey': admin_auth_key,
        'user-agent': 'HTCVRSDET'
    }

    r = requests.put(url = url, json = payload, headers = headers)
    
    if r.status_code != 200:
        print("process_wrapper fail", r.status_code, r.text, r.reason, r.content)
        error_list.append(url + ' failed: ' + str(r.status_code) + '|' + r.reason)
    else:
        print("process_wrapper success")

if __name__ == '__main__':
    print("============================================================")
    print("Sync Prod to Stage")
    print("============================================================")

    '''
    1. init data
    1.1 get prod data 
    1.2 gen stage appId/authorId 
    1.3 add stage review status
    1.4 add to vip content 
    2. chagne prod meta data to stage
    2.1 generate author data
    2.1.1 random author id
    2.1.2 save author info to author db
    2.2 change file 
    2.2.1 download image/video/binary from prod 
    2.2.2 rename files name -> change appId 
    2.2.3 upload files to stage s3
    2.2.3 set stage files path to meta data
    3 save data to stage es draft db
    '''

    # get app id list
    prod_app_id_list = get_prod_app_id_list()
    # prod_app_id_list = esutil.getProdCMSAppList()
    # authorData = esutil.getProdAllAuthor(1546609003,None) + esutil.getProdAllAuthor(1506609002,1546609002) + esutil.getProdAllAuthor(1486609001,1506609001) + esutil.getProdAllAuthor(1486602661,1486609000) + esutil.getProdAllAuthor(None,1486602660)
    
    # get prod app infos
    prod_app_infos   = esutil.getProdCMSdata(prod_app_id_list)
    total = len(prod_app_infos)
    c = 0 
    for prod_app_info_data in prod_app_infos:
        c+=1
        print("Count#   ", c , "/", total)
        prod_app_info   = prod_app_info_data['_source']
        app_id     = prod_app_info['id']
        print("########################################################\n")
        print("processing...", app_id)
        
        check = esutil.getStageApp(app_id, 1)
        if check is not None:
            continue

        # sync author data
        prod_author_id  = prod_app_info['author_id']
        stage_author_id = sync_author_data_to_stage(prod_author_id)

        # addStageReviewStatus
        esutil.updateStageReviewStatus(app_id)

        # add vip
        esutil.addStageVip(app_id)
        
        # sync app history data
        sync_app_history_data_to_stage(app_id)
        
        # sync app data
        sync_app_data_to_stage(prod_app_info)

        time.sleep(1)
        # process wrapper
        process_wrapper(app_id)

    print("wrapper fail list", error_list)
        