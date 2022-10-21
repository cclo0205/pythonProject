import sys
import json
import os
import requests
import time
from datetime import datetime
from elasticsearch import Elasticsearch
from optparse import OptionParser
import uuid

es_prod_url               = "http://localhost:29200" #options.url
es_prod                   = Elasticsearch([es_prod_url],verify_certs=False)
es_prod_cms_app           = "cms_app" #options.index
es_prod_author_index      = "cms_author"
es_prod_cms_history_index = "neo_cms_history"

es_stage_url               = "http://localhost:9200"
es_stage                   = Elasticsearch([es_stage_url],verify_certs=False)
es_stage_cms_app           = "cms_app"
es_stage_vip_index         = "neo_cms_vip"
es_stage_author_index      = "cms_author"
es_stage_cms_history_index = "neo_cms_history"
es_stage_cms_review_status = "neo_cms_review_status"

class esutil:
    def getProdCMSAppList():
        print("getProdCMSAppList")
        try:
            q_body = {
                "_source": ["id"],
                "from":0,
                "size":1200,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"process_status": 6}},    
                            {"term": {"epoch_status": 1}},
                            {"term": {"app_type": 1}}
                        ],
                        "must_not": [
                            {
                                "nested": {
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {
                                                    "term": {
                                                        "extra_docs.attributes.key": "enterprise"
                                                    }
                                                },
                                                {
                                                    "term": {
                                                        "extra_docs.attributes.value": "1"
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    "path": "extra_docs.attributes"
                                }
                            }
                        ]
                }}}
            res = es_prod.search(index=es_prod_cms_app, body=q_body)['hits']['hits']
        except Exception as e:
            print ("get info has error", e)
            sys.exit(0)

        appIds = []
        for each in res:
            appIds.append(each['_source']['id'])
        return appIds

    def getProdCMSdata(app_id_list):
        print("getProdCMSdata")
        try:
            q_body = {
                "from":0,
                "size":1200,
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"id": app_id_list}},
                            {"term": {"epoch_status": 1}}
                        ]
                }}}
            res = es_prod.search(index=es_prod_cms_app, body=q_body)['hits']['hits']
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

    def getProdAllAuthor(gte, lte):
        print("getProdAllAuthor# gte: ", gte, "// lte: ", lte)
        try:
            q_body = {
                "from": 0,
                "size": 10000,
                "query": {"bool": {"must": [
                    {
                        "range": {
                            "update_ts": {
                                "gte": gte,
                                "lte": lte
                                }
                        }
                    }
                ]}}
            }
            res = es_prod.search(index=es_prod_author_index, body=q_body)['hits']['hits']
        except Exception as e:
            print ("getProdAuthor", e)
            sys.exit(3)


        authorInfos = []
        for each in res:
            info = {
                '_source' : each['_source'],
                '_id'    : each['_id'],
            }
            authorInfos.append(info)
        return authorInfos

    def getProdAuthor(prod_author_id):
        print("getProdAuthor# prod_author_id: ",prod_author_id)
        try:
            q_body = {
                "from":0,
                "size":1,
                "query": {"bool": {"must": [{"term": {"id": prod_author_id}}]}}
            }
            res = es_prod.search(index=es_prod_author_index, body=q_body)['hits']['hits']
        except Exception as e:
            print ("getProdAuthor", e)
            sys.exit(3)

        return {
            '_source' : res[0]['_source'],
            '_id'    : res[0]['_id'],
        }


    def addStageVip(prod_app_id):
        print("addStageVip# prod_app_id: ",prod_app_id)
        stage_vip_info = esutil.getStageVip(prod_app_id)
        if stage_vip_info == None:
            print("addStageVip# app_id: ",prod_app_id)
            try:
                doc = {"app_id": prod_app_id, "update_time": 1622045486}
                es_stage.index(index=es_stage_vip_index, body=doc)
            except Exception as e:
                print ("addStageAuthorVip", e)
                sys.exit(1)
        else:
            print("addStageVip# appid is already in stage vip")
        

    def addStageAuthor(prod_author_data):
        prod_author_id = prod_author_data['id']
        stage_author_info = esutil.getStageAuthor(prod_author_id)
        if stage_author_info == None:
            print("addStageAuthor# prod_author_id: ", prod_author_id)
            try:
                doc = { "id": prod_author_id,
                        "name": prod_author_data['name'],
                        "author_desc": prod_author_data['author_desc'],
                        "update_ts": prod_author_data['update_ts'],
                        "contact": prod_author_data['contact'],
                        "create_ts": prod_author_data['create_ts'],
                        "icon": prod_author_data['icon']
                    }
                res = es_stage.index(index=es_stage_author_index, body=doc)
            except Exception as e:
                print ("addStageAuthor", e)
                sys.exit(2)
        else:
            print("addStageAuthor# author is already in stage")


    def getStageAuthor(prod_author_id):
        print("getStageAuthor# prod_author_id: ",prod_author_id)
        try:
            q_body = {
                "from":0,
                "size":1,
                "query": {"bool": {"must": [{"term": {"id": prod_author_id}}]}}
            }
            res = es_stage.search(index=es_stage_author_index, body=q_body)['hits']['hits']
        except Exception as e:
            print ("getProdAuthor", e)
            sys.exit(3)

        if res == []:
            return None
        else:
            return {
                '_source' : res[0]['_source'],
                '_id'    : res[0]['_id'],
            }
        
    def getStageVip(prod_app_id):
        print("getStageVip# prod_app_id: ",prod_app_id)
        try:
            q_body = {
                "from":0,
                "size":1,
                "query": {"bool": {"must": [{"term": {"app_id": prod_app_id}}]}}
            }
            res = es_stage.search(index=es_stage_vip_index, body=q_body)['hits']['hits']
        except Exception as e:
            print ("getStageVip", e)
            sys.exit(4)

        if res == []:
            return None
        else:
            return res[0]

    def addStageApp(prod_app_info):
        prod_app_id = prod_app_info['id']
        prod_app_epoch_status = prod_app_info['epoch_status']
        stage_app_info = esutil.getStageApp(prod_app_id, prod_app_epoch_status)
        if stage_app_info == None:
            print("addStageApp# app_id: ",prod_app_id, "epoch_status", prod_app_epoch_status)
            try:
                doc = prod_app_info
                es_stage.index(index=es_stage_cms_app, body=doc)
            except Exception as e:
                print ("addStageApp", e)
                sys.exit(1)
        else:
            print("addStageApp# appid is already in stage cms_app")

    def getStageApp(prod_app_id, epoch_status):
        print("getStageApp# prod_app_id: ",prod_app_id, "epoch_status: ", epoch_status)
        try:
            q_body = {
                "from":0,
                "size":1,
                "query": {"bool": {"must": [{"term": {"id": prod_app_id}}, {"term": {"epoch_status": epoch_status}}]}}
            }
            res = es_stage.search(index=es_stage_cms_app, body=q_body)['hits']['hits']
        except Exception as e:
            print ("getStageVip", e)
            sys.exit(5)

        if res == []:
            return None
        else:
            return res[0]

    def delStageApp(stage_app_id, epoch_status):
        print("delStageApp# app_id: ", stage_app_id, "epoch_status:", epoch_status)
        stage_app_info = esutil.getStageApp(stage_app_id, epoch_status)
        if stage_app_info == None:
            return []
        else:
            print("delStageApp# app_id: ",stage_app_id, "epoch_status", epoch_status)
            try:
                es_stage.delete(index=es_stage_cms_app, id=stage_app_info['_id'])
            except Exception as e:
                print ("delStageApp", e)
                sys.exit(1)

    def getProdHistory(prod_app_id):
        print("getProdHistory# prod_app_id: ", prod_app_id)
        try:
            q_body = {
                "from":0,
                "size":9999,
                "query": {"bool": {"must": [{"term": {"app_id": prod_app_id}}]}}
            }
            res = es_prod.search(index=es_prod_cms_history_index, body=q_body)['hits']['hits']
        except Exception as e:
            print ("getProdHistory", e)
            sys.exit(5)

        info = []
        for each in res:
            raw = {
                '_source' : each['_source'],
                '_id'    : each['_id'],
            }
            info.append(raw)
        return info

    def getStageHistory(prod_app_id):
        print("getStageHistory# prod_app_id: ", prod_app_id)
        try:
            q_body = {
                "from":0,
                "size":9999,
                "query": {"bool": {"must": [{"term": {"app_id": prod_app_id}}]}}
            }
            res = es_stage.search(index=es_prod_cms_history_index, body=q_body)['hits']['hits']
        except Exception as e:
            print ("getStageHistory", e)
            sys.exit(5)

        info = []
        for each in res:
            raw = {
                '_source' : each['_source'],
                '_id'    : each['_id'],
            }
            info.append(raw)
        return info
    
    def addStageHistory(history_info):
        app_id = history_info['app_id']
        print("addStageHistory# app_id: ", app_id)
        stage_app_history_info = esutil.getStageHistory(app_id)
        if stage_app_history_info == []:
            try:
                print("addStageHistory# app_id: ", app_id)
                doc = history_info
                es_stage.index(index=es_stage_cms_history_index, body=doc)
            except Exception as e:
                print ("addStageHistory", e)
                sys.exit(1)
        else:
            print("addStageHistory# appid is already in stage cms history")

    def getStageReviewStatus(app_id):
        print("getStageReviewStatus# app_id: ", app_id)
        try:
            q_body = {
                "from":0,
                "size":1,
                "query": {"bool": {"must": [{"term": {"app_id": app_id}}]}}
            }
            res = es_stage.search(index=es_stage_cms_review_status, body=q_body)['hits']['hits']
        except Exception as e:
            print ("getStageReviewStatus", e)
            sys.exit(5)

        info = []
        for each in res:
            raw = {
                '_source' : each['_source'],
                '_id'    : each['_id'],
            }
            info.append(raw)
        return info[0]
    
    def addStageReviewStatus(app_id):
        print("addStageReviewStatus# app_id: ", app_id)
        stage_review_status_info = esutil.getStageReviewStatus(app_id)
        if stage_review_status_info == []:
            try:
                ReviewStatusBody = {
                        "app_id": app_id,
                        "release_channel": "prod",
                        "s3sync": 201,
                        "cv": 201,
                        "review": 201,
                        "wrapper": 100,
                        "payment": 100,
                        "publish": 100
                    }
                doc = ReviewStatusBody
                es_stage.index(index=es_stage_cms_review_status, body=doc)
            except Exception as e:
                print ("addStageReviewStatus", e)
                sys.exit(1)
        else:
            print("addStageReviewStatus# appid is already in stage reviewStatus")

    def updateStageReviewStatus(app_id):
        print("updateStageReviewStatus# app_id: ", app_id)
        stage_review_status_info = esutil.getStageReviewStatus(app_id)
        print(stage_review_status_info)
        doc_id = stage_review_status_info["_id"]
        try:
            doc = {
                "doc":{
                    "review": 201,
                    "wrapper": 100,
                    "payment": 100,
                    "publish": 100
                }
            }
            es_stage.update(index=es_stage_cms_review_status, doc_type= "_doc", id=doc_id, body=doc)
        except Exception as e:
            print ("addStageReviewStatus", e)
            sys.exit(1)
