#!/usr/local/bin/python3
# -*- coding: utf8 -*-

import copy
import json
import time
from elasticsearch import Elasticsearch
import sys

if len(sys.argv) != 5:
    print("Usage: %s <es_host/ip:port> <source_index_name> <new_index_name> <timestamp>" % sys.argv[0])
    sys.exit(-1)

es_url = "http://" + sys.argv[1] + "/"
es = Elasticsearch([es_url], verify_certs=False)

src_index = sys.argv[2]
dst_index = sys.argv[3]

q_body = {
  "from": 0,
  "size": 9999,
  "query": {
    "bool": {
      "must": [
        {
          "range": {
            "update_time": {
                "gte": sys.argv[4]
            }
          }
        }
      ]
    }
  }
}

res = es.search(index=src_index,
                body=q_body)['hits']['hits']

print("updated count:", len(res))
print("=====")

def getApp(app_id):
    q_body = {
        "from": 0,
        "size": 1,
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "id": app_id
                        }
                    },
                    {
                        "term": {
                            "epoch_status": 0
                        }
                    }
                ]
            }
        }
    }

    res = es.search(index=dst_index, body=q_body)['hits']['hits']
    return res[0]["_source"]


for s in res:
    _id = s["_id"]
    id = s["_source"]["id"]
    s["_source"]["binary_group_id"] = ''
    if 'auto_published_new_countries' in s["_source"]:
        del s["_source"]["auto_published_new_countries"]
    if 'purchase_count' in s["_source"]:
        del s["_source"]["purchase_count"]
    if 'thumbnails' in s["_source"]:
        if 'media_type' in s["_source"]["thumbnails"]:
            del s["_source"]["thumbnails"]["media_type"]
    if 'wrapper_service_status' in s["_source"]:
        del s["_source"]["wrapper_service_status"]
    if 'category_ids' in s["_source"]:
        del s["_source"]["category_ids"]
    if 'edu_series' in s["_source"]:
        del s["_source"]["edu_series"]
    if 'edu_duration' in s["_source"]:
        del s["_source"]["edu_duration"]
    
    bodybody = {"doc": s["_source"], "doc_as_upsert":True}
    es.update(index=dst_index, doc_type="_doc", id=_id, body=bodybody)
    print(id, "has updated")
    check = getApp(id)
    print("check data =>", check == s["_source"])
    print("-----")

