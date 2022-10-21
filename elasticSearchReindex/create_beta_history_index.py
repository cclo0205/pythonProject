#!/usr/local/bin/python3
# -*- coding: utf8 -*-

import sys
import json
import requests

if len(sys.argv) != 4:
    print("Usage: %s <es_host/ip:port> <beta_history_index_name> <mapping_file_name>" % sys.argv[0])
    sys.exit(-1)

es_url = "http://" + sys.argv[1] + "/"
beta_history_index_name = sys.argv[2]
mapping_file_name = sys.argv[3]

def delIndex(index_name):
    url = es_url + index_name
    requests.delete(url)

def createHistoryIndex(index, mapping_file_name):
    print(mapping_file_name)
    with open(mapping_file_name) as mapping_file:
        new_mapping = json.load(mapping_file)
    
    url = es_url + index
    response = requests.put(url, json=new_mapping)
    print("Create index", index, "Result:", response.status_code, response.content)


if __name__ == '__main__':
    delIndex(beta_history_index_name)
    createHistoryIndex(beta_history_index_name, mapping_file_name)

