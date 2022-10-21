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

# usage = "usage: %prog -p"

# parser = OptionParser(usage=usage)
# parser.add_option("-u", "--url", action="store", type="string", dest="url", help="url")
# parser.add_option("-i", "--index", action="store", type="string", dest="index", help="ES_INDEX_PUBLISH")

# (options, args) = parser.parse_args()
# es_url                = options.url
# ES_INDEX_PUBLISH      = options.index

es_url                = "http://10.222.142.193:9200"
ES_INDEX_PUBLISH      = "neo_cms_beta_v1"

es                 = Elasticsearch([es_url],verify_certs=False)
ES_TYPE_APP        = "_doc"
FILTER_PATH        = ["id", "_id", "epoch_status", "create_time", "release_time"]

for index, each in enumerate(FILTER_PATH):
    FILTER_PATH[index] = "hits.hits._source."+each
    if(each=="_id"): FILTER_PATH[index] = "hits.hits._id"

ids = []

def getESdata():
    try:
        q_body = {
            "from":0,
            "size":9999,
            "query": {
                "bool": {"must": [
                    {
                        "term":{
                            "app_type":1
                        }
                    }
                ]}
                    }
        }
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

def updateCmsApp(app_info_list):
    print(len(app_info_list), "found.")
    for app in app_info_list:
        doc_id = app['_id']
        app_id = app['_source']['id']
        create_time = app['_source']['create_time']
        release_time = app['_source']['release_time']
        if (len(str(create_time))== 13 | len(str(release_time))==13 ):
            print("ts > 13, app_id: ", app_id)
            print("create_time", create_time, "release_time", release_time)
        else:
            continue

        new_create_time = int(create_time/1000)
        new_release_time = int(release_time/1000)
        print("new_create_time", new_create_time, "new_release_time", new_release_time)

        bodybody = {"doc": {
            "create_time": new_create_time,
            "release_time": new_release_time
            }
        }
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
    print("Migrate CreateTS/ReleaseTS 13to10:")
    print("============================================================")
    updateCmsApp(getESdata())