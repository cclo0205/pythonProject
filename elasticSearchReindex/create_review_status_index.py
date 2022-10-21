#!/usr/local/bin/python3
# -*- coding: utf8 -*-

import sys
import json
import requests
from elasticsearch import Elasticsearch

if len(sys.argv) != 6:
    print("Usage: %s <es_host/ip:port> <prod_index_name> <beta_index_name> <review_status_index_name> <mapping_file_name>" % sys.argv[0])
    sys.exit(-1)

es_url = "http://" + sys.argv[1] + "/"
es = Elasticsearch([es_url], verify_certs=False)
filter = ["hits.hits._source.id", "hits.hits._id", "hits.hits._source.epoch_status", "hits.hits._source.process_status"]
prod_index_name = sys.argv[2]
beta_index_name = sys.argv[3]
review_status_index_name = sys.argv[4]
mapping_file_name = sys.argv[5]

def delIndex(index_name):
    url = es_url + index_name
    requests.delete(url)

def createReviewStatusIndex(index, mapping_file_name):
    print(mapping_file_name)
    with open(mapping_file_name) as mapping_file:
        new_mapping = json.load(mapping_file)
    
    url = es_url + index
    response = requests.put(url, json=new_mapping)
    print("Create index", index, "Result:", response.status_code, response.content)

def queryApps(app_index, release_channel, review_status_index):
    q_body = {
        "from": 0,
        "size": 9999,
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "epoch_status": 0
                        }
                    }
                ]
            }
        }
    }

    res = es.search(index=app_index,
                    body=q_body,
                    filter_path=filter)['hits']['hits']

    size = len(res)
    for i in range(size):
        app_id = res[i]["_source"]["id"]
        process_status = res[i]["_source"]["process_status"]
        createReviewStatusData(size, i, review_status_index, app_id, release_channel, process_status)


def createReviewStatusData(size, idx, review_status_index, app_id, release_channel, process_status: int):
    if release_channel == "prod":
        reviewWait, reviewSuccess = 100, 101
    else:
        reviewWait, reviewSuccess = 200, 201
    if process_status == -7:    #s3sync failed
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 101,
            "cv": 100,
            "review": reviewWait,
            "wrapper": 100,
            "payment": 100,
            "publish": 100
        }
    elif process_status == -6:  #publish failed
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 101,
            "cv": 101,
            "review": reviewSuccess,
            "wrapper": 101,
            "payment": 101,
            "publish": 102
        }
    elif process_status == -5:  #cv failed
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 101,
            "cv": 102,
            "review": reviewWait,
            "wrapper": 100,
            "payment": 100,
            "publish": 100
        }
    elif process_status == -4 or process_status == -1:  #unlock, rejected, don't care
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 100,
            "cv": 100,
            "review": reviewWait,
            "wrapper": 100,
            "payment": 100,
            "publish": 100
        }
    elif process_status == -3:  #payment failed
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 101,
            "cv": 101,
            "review": reviewSuccess,
            "wrapper": 101,
            "payment": 102,
            "publish": 100
        }
    elif process_status == -2:  #wrapper failed
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 101,
            "cv": 101,
            "review": reviewSuccess,
            "wrapper": 102,
            "payment": 100,
            "publish": 100
        }
    elif process_status == 0 or process_status == 12:   #submitted, process s3sync
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 100,
            "cv": 100,
            "review": reviewWait,
            "wrapper": 100,
            "payment": 100,
            "publish": 100
        }
    elif process_status == 1 or process_status == 10 or process_status in [9001,9002,9003,9004]:   #reviewing, cv success
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 101,
            "cv": 101,
            "review": reviewWait,
            "wrapper": 100,
            "payment": 100,
            "publish": 100
        }
    elif process_status == 2 or process_status == 3:   #approved, process wrapper
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 101,
            "cv": 101,
            "review": reviewSuccess,
            "wrapper": 100,
            "payment": 100,
            "publish": 100
        }
    elif process_status == 4 or process_status == 8:   #process payment, wrapper success
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 101,
            "cv": 101,
            "review": reviewSuccess,
            "wrapper": 101,
            "payment": 100,
            "publish": 100
        }
    elif process_status == 5 or process_status == 11:   #ready to publish, processing publish
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 101,
            "cv": 101,
            "review": reviewSuccess,
            "wrapper": 101,
            "payment": 101,
            "publish": 100
        }
    elif process_status == 6 or process_status == 7:   #publish, unpublish
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 101,
            "cv": 101,
            "review": reviewSuccess,
            "wrapper": 101,
            "payment": 101,
            "publish": 101
        }
    elif process_status == 9 or process_status == 13:   #processing cv, s3sync success
        reviewStatus = {
            "app_id": app_id,
            "release_channel": release_channel,
            "s3sync": 101,
            "cv": 100,
            "review": reviewWait,
            "wrapper": 100,
            "payment": 100,
            "publish": 100
        }
    else:
        print("######Error:", app_id, "processStatus:", process_status)

    url = es_url + review_status_index + "/_doc/"
    response = requests.post(url, json = reviewStatus)
    print("[%s]" % release_channel.upper(), "[%d/%d]" % (size, idx+1), "Insert", review_status_index, "AppId", app_id, "processStatus:", process_status, "Result:", response.status_code, response.content)

if __name__ == '__main__':
    delIndex(review_status_index_name)
    createReviewStatusIndex(review_status_index_name, mapping_file_name)
    queryApps(prod_index_name, "prod", review_status_index_name)
    queryApps(beta_index_name, "beta", review_status_index_name)

