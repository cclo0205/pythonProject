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

usage = "usage: %prog -u es_url -i index_name -c new_country_list_string"

parser = OptionParser(usage=usage)
parser.add_option("-u", "--url", action="store", type="string", dest="url", help="url")
parser.add_option("-i", "--index", action="store", type="string", dest="index", help="ES_INDEX_PUBLISH")
parser.add_option("-c", "--country", action="store", type="string", dest="country", help="new country list")

(options, args) = parser.parse_args()
es_url                = options.url
ES_INDEX_PUBLISH      = options.index
country_keys          = options.country.split(",")

es                 = Elasticsearch([es_url],verify_certs=False)
ES_TYPE_APP        = "_doc"
FILTER_PATH        = ["id", "_id", "epoch_status", "country","auto_publish_new_country"]

#country_keys = ["QA", "KW"]
outfile = open("appIds", 'w')


for index, each in enumerate(FILTER_PATH):
    FILTER_PATH[index] = "hits.hits._source."+each
    if(each=="_id"): FILTER_PATH[index] = "hits.hits._id"

def getESdata():
    try:
        q_body = {"from":0,"size":9999,"query": {"bool": {"must": [{"term": {"auto_publish_new_country": "1"}}]}}}
        res = es.search(index=ES_INDEX_PUBLISH, 
                        body=q_body,
                        filter_path=FILTER_PATH)['hits']['hits']
    except Exception as e:
        print ("get info has error", e)
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
    update_count = 0
    for app in app_info_list:
        doc_id = app['_id']
        app_id = app['_source']['id']

        origin_country = app["_source"]["country"]
        new_country = copy.deepcopy(origin_country)

        for country_key in country_keys:
            if country_key not in origin_country:
                new_country.append(country_key)

        if(new_country != origin_country):
            print("============update country============")
            print(app_id, "origin_country:", json.dumps(origin_country), "new_country:", json.dumps(new_country))
            bodybody = {"doc": {"country": new_country}}
            #print bodybody
            es.update(index=ES_INDEX_PUBLISH, doc_type=ES_TYPE_APP, id=doc_id, body=bodybody)
            update_count += 1
            outfile.write(app_id + '\n')
    print('update_count:%d'%update_count)
    outfile.close()

if __name__ == '__main__':
    print("============================================================")
    print("Migrate CMS_App country:")
    updateCmsApp(getESdata())
    print("============================================================")
