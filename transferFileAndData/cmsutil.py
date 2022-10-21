import sys
import json
import os
import requests
import time
from datetime import datetime
from elasticsearch import Elasticsearch
from optparse import OptionParser
import uuid
from s3util import s3util

class cmsutil:
    def syncThumbnailsFile(thumbnails):
        for key in thumbnails:
            url = thumbnails[key]['url']
            stage_s3_url = cmsutil.processFileSyncStage(url)
            
            # gen stage s3 url 
            thumbnails[key]['url'] = stage_s3_url

        return thumbnails

    def syncCloudFile(cloud):
        objs = cloud['objs']
        new_objs = []
        for obj in objs:
            url = "https://assets-global.viveport.com/vr_developer_published_assets/" + obj['prefix_path'] + obj['name']
            cmsutil.processFileSyncStage(url)
            obj['bucket'] = "htc-vr-stage-usw2-cms-submitted-80320"
            new_objs.append(obj)
        
        new_cloud = {"objs": new_objs}
        return new_cloud

    def syncGalleryFile(gallery):
        new_gallery = []
        for media in gallery:
            url = media['url']
            stage_s3_url = cmsutil.processFileSyncStage(url)
            media['url'] = stage_s3_url
            
            if "cover" in media:
                cover = media['cover']
                if cover != None:
                    stage_s3_cover_url = cmsutil.processFileSyncStage(cover)
                    media['cover'] = stage_s3_cover_url
            
            new_gallery.append(media)
        return new_gallery

    def syncBinaryFile(appId, binary, appType):
        local_file_path = s3util.downloadBinaryFile(appId, appType, binary)
        # getStageS3Path
        s3_file_path = s3util.getFileInfo(binary)['s3_file_path']
        
        # uploadFile
        stage_s3_url = s3util.uploadFileToS3(local_file_path, "htc-vr-stage-usw2-cms-submitted-80320", s3_file_path)
        return stage_s3_url

    def processFileSyncStage(url):
        # downloadFile
        local_file_path = s3util.downloadFile(url)
        
        # getStageS3Path
        s3_file_path = s3util.getFileInfo(url)['s3_file_path']
        
        # uploadFile
        stage_s3_url = s3util.uploadFileToS3(local_file_path, "htc-vr-stage-usw2-cms-submitted-80320", s3_file_path)
        return stage_s3_url
