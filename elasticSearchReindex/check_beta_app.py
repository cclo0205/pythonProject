#!/usr/local/bin/python3
# -*- coding: utf8 -*-

import copy
import json
import time
from elasticsearch import Elasticsearch
import sys

if len(sys.argv) != 6:
    print("Usage: %s <es_host/ip:port> <source_index_name> <source_publish_index_name> <new_index_name> <timestamp>" % sys.argv[0])
    sys.exit(-1)

es_url = "http://" + sys.argv[1] + "/"
es = Elasticsearch([es_url], verify_certs=False)

src_index = sys.argv[2]
src_pub_index = sys.argv[3]
dst_index = sys.argv[4]

q_body = {
  "from": 0,
  "size": 9999,
  "query": {
    "bool": {
      "must": [
        {
          "range": {
            "update_time": {
                "gte": sys.argv[5]
            }
          }
        }
      ]
    }
  }
}

res = es.search(index=src_index,
                body=q_body)['hits']['hits']
res_pub = es.search(index=src_pub_index,
                body=q_body)['hits']['hits']

def getApp(app_id, epoch_status):
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
                            "epoch_status": epoch_status
                        }
                    }
                ]
            }
        }
    }

    res = es.search(index=dst_index, body=q_body)['hits']['hits']
    return res[0]["_source"]

print("beta-review updated count:", len(res))
print("=====")
for s in res:
    _id = s["_id"]
    id = s["_source"]["id"]
    s["_source"]["binary_group_id"] = ''
    s["_source"]["epoch_status"] = 0
    if 'release_type' in s["_source"]:
        del s["_source"]["release_type"]
    if 'payment_status' in s["_source"]:
        del s["_source"]["payment_status"]
    if 'category_ids' in s["_source"]:
        del s["_source"]["category_ids"]
    if 'edu_series' in s["_source"]:
        del s["_source"]["edu_series"]
    if 'edu_duration' in s["_source"]:
        del s["_source"]["edu_duration"]
    
    bodybody = {"doc": s["_source"], "doc_as_upsert":True}
    es.update(index=dst_index, doc_type="_doc", id=_id, body=bodybody)
    print(id, "has updated")
    check = getApp(id, 0)
    print("check data =>", check == s["_source"])
    print("-----")

print("beta-market updated count:", len(res_pub))
print("=====")
for s in res_pub:
    _id = s["_id"]
    id = s["_source"]["id"]
    s["_source"]["binary_group_id"] = ''
    s["_source"]["epoch_status"] = 1
    if 'release_type' in s["_source"]:
        del s["_source"]["release_type"]
    if 'payment_status' in s["_source"]:
        del s["_source"]["payment_status"]
    if 'category_ids' in s["_source"]:
        del s["_source"]["category_ids"]
    if 'edu_series' in s["_source"]:
        del s["_source"]["edu_series"]
    if 'edu_duration' in s["_source"]:
        del s["_source"]["edu_duration"]

    bodybody = {"doc": s["_source"], "doc_as_upsert":True}
    es.update(index=dst_index, doc_type="_doc", id=_id, body=bodybody)
    print(id, "has updated (beta-market)")
    check = getApp(id, 1)
    print("check data =>", check == s["_source"])
    print("-----")
