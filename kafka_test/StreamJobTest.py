#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/23 下午2:58
# @Author  : Sundiandong
# @Site    : 
# @File    : StreamJobTest.py
# @Software: PyCharm


from StreamJobConf import *
from TableGroupTest import *
import httplib

class StreamJobTest():
    def __init__(self):
        dic ={}
        self.topicName = TopicName
        self.storageFormat = StorageFormat
        self.transactionSchema = TransactionSchema
        self.targetPrn = TargetPrn
        self.jobType = JobType
        self.prn = Prn
        self.kafkaConnect = KafkaConnect
        self.config = Config
        self.schedule = Schedule

        dic["topicName"] = self.topicName
        dic["storageFormat"] = self.storageFormat
        dic["transactionSchema"] = self.transactionSchema
        dic["targetPrn"] = self.targetPrn
        dic["jobType"] = self.jobType
        dic["prn"] = self.prn
        dic["kafkaConnect"] = self.kafkaConnect
        dic["config"] = self.config
        dic["schedule"] = self.schedule
        self.postjson = json.dumps(dic,sort_keys=True,indent=4,separators=(',',':'))
        # 查看创建streamjob的post body json
        # print self.postjson

    def StreamJobCreate(self):
        self.headerdata = {"Content-type": "application/json",
                           "Cookie":"User-Token=648e618b-854b-4de7-bbf3-9b408fd32d86"}

        # testenv环境创建
        # self.requrl = "http://gateway.prophetee.test.testenv.4pd.io/telamon/v1/stream-jobs"
        # conn = httplib.HTTPConnection("172.27.128.73", 80)

        #prophetee-qb环境创建
        self.requrl = "http://gateway.prophetee-qb.ci.4pd.io/telamon/v1/stream-jobs"
        conn = httplib.HTTPConnection("172.27.129.31", 80)

        conn.request('POST', self.requrl, self.postjson, self.headerdata)
        response = conn.getresponse()
        res = response.read()
        conn.close()
        print "创建流式任务：\n" + res





