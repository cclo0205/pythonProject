#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import json
import time
import os
import copy
from datetime import datetime
from elasticsearch import Elasticsearch
from optparse import OptionParser
import requests

usage = "usage: %prog -p"

parser = OptionParser(usage=usage)
parser.add_option("-u", "--url", action="store", type="string", dest="url", help="url")
parser.add_option("-i", "--index", action="store", type="string", dest="index", help="ES_INDEX_PUBLISH")

(options, args) = parser.parse_args()
es_url                = options.url
ES_INDEX_PUBLISH      = options.index

es                 = Elasticsearch([es_url],verify_certs=False)
ES_TYPE_APP        = "_doc"
FILTER_PATH        = ["id", "_id", "epoch_status", "process_status", "app_settings", "app_type"]

git_status = False
stroelist_key = "playinfinity"  # Define stroelist_key here
txt_file = "storelist.txt"

for index, each in enumerate(FILTER_PATH):
    FILTER_PATH[index] = "hits.hits._source."+each
    if(each=="_id"): FILTER_PATH[index] = "hits.hits._id"

ids = []

def getESdata(epoch_status):
    try:
        q_body = {"from":0,"size":9999,"query": {"bool": {"must": [{"term": {"epoch_status": epoch_status}},{"terms":{"id":ids}}]}}}
        res = es.search(index=ES_INDEX_PUBLISH, 
                        body=q_body,
                        filter_path=FILTER_PATH)['hits']['hits']
    except Exception as e:
        print("get info has error", e)
        sys.exit(0)

    info = []
    for each in res:
        raw = {
            '_source' : each['_source'],
            '_id'    : each['_id'],
        }
        info.append(raw)
    return info

def updateCmsApp(app_info_list, epoch_status):
    print(len(app_info_list), "found.")
    for app in app_info_list:
        doc_id = app['_id']
        app_id = app['_source']['id']
        app_type = app['_source']['app_type']
        app_list = getapplist(txt_file)

        origin_appsettings = app["_source"]['app_settings']

        if "store_list" not in origin_appsettings:
            origin_appsettings["store_list"] = []
        if not isinstance(origin_appsettings["store_list"], list):
            origin_appsettings["store_list"] = []

        new_appsettings = copy.deepcopy(origin_appsettings)

        if app_id in app_list:
            if stroelist_key not in origin_appsettings["store_list"]:
                new_appsettings["store_list"].append(stroelist_key)
        else:
            if stroelist_key in origin_appsettings["store_list"]:
                new_appsettings["store_list"].remove(stroelist_key)

        if(new_appsettings != origin_appsettings):
            print(app_id, "origin_appsettings:", json.dumps(origin_appsettings), "new_appsettings:", json.dumps(new_appsettings))
            bodybody = {"doc": {"app_settings": new_appsettings}}
            print(bodybody)
            es.update(index=ES_INDEX_PUBLISH, doc_type=ES_TYPE_APP, id=doc_id, body=bodybody)

def getapplist(txt_file):
    source = set()
    with open(txt_file, "r") as f:
        for l in f.readlines():
            source.add(l.strip())
    return list(source)

if __name__ == '__main__':
    print("============================================================")
    print("Migrate CMS_App AppSetting StroeList:")
    print("============================================================")
    updateCmsApp(getESdata(epoch_status = 1),epoch_status = 1)
    updateCmsApp(getESdata(epoch_status = 0),epoch_status = 0)