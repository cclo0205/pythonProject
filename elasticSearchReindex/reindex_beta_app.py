#!/usr/local/bin/python3
# -*- coding: utf8 -*-

import sys
import json
import requests
from elasticsearch import Elasticsearch

if len(sys.argv) != 7:
    print("Usage: %s <es_host/ip:port> <source_index_name> <source_publish_index_name> <new_index_name> <alias_name> <mapping_file_name>" % sys.argv[0])
    sys.exit(-1)

es_url = "http://" + sys.argv[1] + "/"
es = Elasticsearch([es_url], verify_certs=False)
source_index_name = sys.argv[2]
source_publish_index_name = sys.argv[3]
target_index_name = sys.argv[4]
alias_name = sys.argv[5]
file_name = sys.argv[6]
print(file_name)
with open(file_name) as mapping_file:
    print(mapping_file)
    new_mapping = json.load(mapping_file)


def delIndex(index_name):
    url = es_url + index_name
    requests.delete(url)

def checkAlias(index_name, name):
    url = es_url + "_aliases"
    all_alias = requests.get(url).json()
    if index_name in all_alias:
        if name in all_alias[index_name]['aliases']:
            print("["+index_name+"] alias: " + name + " checked.")
        else:
            print("Alias name not match, real:", json.dumps(list(all_alias[index_name]['aliases'].keys())), "provided:", name)
            sys.exit(-1)

def changeAlias(index1, index2, name):
    data = {
            "actions" : [
                { "remove" : { "index" : index1, "alias" : name } },
                { "add" : { "index" : index2, "alias" : name } }
            ]}
    url = es_url + "_aliases"
    print("Change alias", name, "from", index1, "to", index2, ", Result:", requests.post(url, json=data).status_code)

def reindex(index1, publish_index1, index2):
    url = es_url + index2
    response = requests.put(url, json=new_mapping)
    print("Create index", index2, "Result:", response.status_code, response.content)
    url = es_url + "_reindex"
    data = {"source":{"index":index1},"dest":{"index":index2},"script":{"source": "ctx._source.binary_group_id = ''; ctx._source['epoch_status'] = 0; ctx._source.remove('release_type'); ctx._source.remove('payment_status'); ctx._source.remove('category_ids'); ctx._source.remove('edu_series'); ctx._source.remove('edu_duration')","lang":"painless"}}
    response = requests.post(url, json = data)
    print("Reindex", index1, "to", index2, "Result:", response.status_code, response.content)
    
    data = {"source":{"index":publish_index1},"dest":{"index":index2},"script":{"source": "ctx._source.binary_group_id = ''; ctx._source['epoch_status'] = 1; ctx._source.remove('release_type'); ctx._source.remove('payment_status'); ctx._source.remove('category_ids'); ctx._source.remove('edu_series'); ctx._source.remove('edu_duration')","lang":"painless"}}
    response = requests.post(url, json = data)
    print("Reindex", publish_index1, "to", index2, "Result:", response.status_code, response.content)


def queryES(epoch_status):
    q_body = {
        "from": 0,
        "size": 9999,
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "epoch_status": epoch_status
                        }
                    }
                ]
            }
        }
    }
    res = es.search(index=target_index_name, body=q_body)['hits']['hits']
    return res

def createReviewData():
    res = queryES(0)
    res_pub = queryES(1)
    size = len(res_pub)
    print("review:", len(res), "publish:", size)
    idx = 1
    for r_pub in res_pub:
        found = False
        for r in res:
            if r["_source"]["id"] == r_pub["_source"]["id"]:
                print("[%d/%d]" % (idx, size), r_pub["_source"]["id"], "skip")
                found = True
                break
        if not found:
            #bodybody = {"doc": r_pub["_source"], "doc_as_upsert":True}
            r_pub["_source"]["epoch_status"] = 0
            es.index(index=target_index_name, body=r_pub["_source"])
            print("[%d/%d]" % (idx, size), r_pub["_source"]["id"], "insert review data")
        idx += 1

if __name__ == '__main__':
    delIndex(target_index_name)
    #checkAlias(source_index_name, alias_name)
    reindex(source_index_name, source_publish_index_name, target_index_name)
    #changeAlias(source_index_name, target_index_name, alias_name)
    createReviewData()
