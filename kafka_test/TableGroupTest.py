#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/23 下午6:03
# @Author  : Sundiandong
# @Site    : 
# @File    : TableGroupTest.py
# @Software: PyCharm


from TableGroupConf import *
import httplib,random,string,json

class TableGroupTest():
    def __init__(self):
        self.postjson = PostJson

    def TableGroupCreate(self):
        self.headerdata = {"Content-type": "application/json",
                           "Cookie": "User-Token=648e618b-854b-4de7-bbf3-9b408fd32d86"}
        # 在testenv环境创建
        # self.requrl = "http://gateway.prophetee.test.testenv.4pd.io/telamon/v1/stream-jobs"
        # conn = httplib.HTTPConnection("172.27.128.73", 80)

        # 在prophetee-qb环境创建
        self.requrl = "http://gateway.prophetee-qb.ci.4pd.io/telamon/v1/table-groups"
        conn = httplib.HTTPConnection("172.27.129.31", 80)
        conn.request('POST', self.requrl, self.postjson, self.headerdata)
        response = conn.getresponse()
        res = response.read()
        conn.close()
        print "创建TableGroup：\n" + res


# tg = TableGroupTest()
# print tg.postjson
# tg.TableGroupCreate()