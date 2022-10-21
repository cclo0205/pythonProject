import copy
from email.quoprimime import body_check
import json
from pickle import TRUE
import re
import time
import sys
import requests
from termcolor import colored
from elasticsearch import Elasticsearch
import csv
from datetime import datetime

admin_host = ''
authkey = ""

es_url             = ""
ES_INDEX_PUBLISH      = ""
es                 = Elasticsearch([es_url],verify_certs=False)
FILTER_PATH        = ["id", "_id", "epoch_status", "process_status", "app_type", "country", "genre_ids", "title", "publish_date","extra_docs", "is_free", "price", "category_id"]

for index, each in enumerate(FILTER_PATH):
    FILTER_PATH[index] = "hits.hits._source."+each
    if(each=="_id"): FILTER_PATH[index] = "hits.hits._id"

def getESdata():
    try:
        q_body = {"from":0,"size":9999,"query": {"bool": {
          "must": [
            {"term":{"epoch_status": 1}},
            {"term":{"app_type": 1}},
            {"nested": {"query": {"bool": {"must": [{"terms": {"extra_docs.attributes.key": ["viveport","subscription"]}},{"term": {"extra_docs.attributes.value": "1"}}]}},"path": "extra_docs.attributes"}}
            ],"must_not": []}}}
        res = es.search(index=ES_INDEX_PUBLISH, 
                        body=q_body,
                        filter_path=FILTER_PATH)['hits']['hits']
    except Exception as e:
        print("get info has error", e)
        sys.exit(0)

    appIds = []
    for each in res:
        appIds.append(each['_source']['id'])
    return appIds

def getESdataByAppIds(appIds):
    try:
        q_body = {"from":0,"size":1000,"query": {"bool": {
          "must": [
            # {"term": {"country": "US"}},
            {"term": {"process_status": 6}},
            {"term":{"epoch_status": 1}},
            {"terms":{"id": appIds}}
          ],
          "must_not": []}}
        }
        res = es.search(index=ES_INDEX_PUBLISH, 
                        body=q_body,
                        filter_path=FILTER_PATH)['hits']['hits']
    except Exception as e:
        print("get info has error", e)
        sys.exit(0)

    appInfos = []
    for each in res:
        appInfos.append(each['_source'])
    return appInfos

def getListModelAppIdList(modelId, f):
  body = {
      "modelId": modelId,
      "from": f,
      "size": 100
  }
  headers = {'AuthKey': authkey}
  url = admin_host + "api/blacklist/v1/admin/listModelApp"
  res = requests.post(url=url, headers=headers, json=body, timeout=5)
  if (res.status_code!=200):
      print("get model app list error:",colored(res.content,'red'))
      return []

  content = res.json()
  appIds = []
  for modelApp in content['modelApps']:
    appIds.append(modelApp['app']['appId'])
  return appIds

def categoryIdsStr(genre_ids):
    list = [x for x in genre_ids if int(x) >= 1000]
    return ",".join(list)

def USDPrice(source):
    p = 0.0
    if 'price' in source:
        for each in source['price']:
            if each['currency'] == "USD":
                p = each['price']
    return p

def CNYPrice(source):
    p = 0.0
    if 'price' in source:
        for each in source['price']:
            if each['currency'] == "CNY":
                p = each['price']
    return p

def OptInV(extra_docs):
  res = False
  for doc in extra_docs:
    if 'key' in doc:
      if doc['key'] == 'stores':
        for attri in doc['attributes']:
          if attri['key'] == "viveport" and str(attri['value']) == "1":
            res = True
  return res

def OptInS(extra_docs):
  res = False
  for doc in extra_docs:
    if 'key' in doc:
      if doc['key'] == 'stores':
        for attri in doc['attributes']:
          if attri['key'] == "subscription" and str(attri['value']) == "1":
            res = True
  return res

def Category(c):
  if c == "0":
    return "Game"
  return "App"

def ChinaStore(c):
  if "CN" in c:
    return True
  return False

def GlobalStore(c):
  if "CN" not in c:
    return True
  return False

def CAG(c):
  cc = ChinaStore(c)
  gg = GlobalStore(c)
  if cc == True and gg == True:
    return "China and Global"
  if cc == True and gg == False:
    return "China"
  if cc == False and gg == True:
    return "Global"

def PT(i_f):
  if i_f == True:
    return "Free"
  return "Paid"

def CNN(cnn):
  if cnn == None:
    return ""
  return cnn

if __name__ == "__main__":
  cmsMvrAppIdList = getESdata()

  modelId = "78f1fef2-a23f-445f-a047-9a7bf515ba9a"
  blackListAppIdList = []
  for i in range(0, 1000, 100):
    blackListAppIdList.extend(getListModelAppIdList(modelId, i))

  headsetWhiteListAppList = []
  for appId in cmsMvrAppIdList:
    if appId not in blackListAppIdList:
      headsetWhiteListAppList.append(appId)

  print(headsetWhiteListAppList)
  print("headsetWhiteListAppList len",len(headsetWhiteListAppList))
  viveFlowInfos = getESdataByAppIds(headsetWhiteListAppList)
  print("viveFlowInfos len",len(viveFlowInfos))
  with open('0919viveFocusPlusApps.csv', mode='w') as csv_file:
    fieldnames = ['ID', 'Title Name (EN)', 'Title Name (CN)', 'Category (Game / App)', 'Genre', 'Viveport Store Opt-in (Viveport Single Purchase)', 
    'Subscription Opt-in', 'Last Publish Date', 'Price Type', 'USD Price', 'CNY Price', 'Publish Country', 'Publish Region']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    
    for info in viveFlowInfos:
      writer.writerow(
        {
          'ID': info['id'],
          'Title Name (EN)': info['title']['en_us'],
          'Title Name (CN)': CNN(info['title']['zh_cn']),
          'Category (Game / App)': Category(info['category_id']),
          'Genre': info['genre_ids'],
          'Viveport Store Opt-in (Viveport Single Purchase)': OptInV(info['extra_docs']),
          'Subscription Opt-in': OptInS(info['extra_docs']),
          'Last Publish Date': info['publish_date'],
          'Price Type': PT(info['is_free']),
          'USD Price': USDPrice(info),
          'CNY Price': CNYPrice(info),
          'Publish Country': info['country'],
          'Publish Region': CAG(info['country'])
        }
      )


