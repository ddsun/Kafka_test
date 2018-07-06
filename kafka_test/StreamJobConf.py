#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/23 下午3:11
# @Author  : Sundiandong
# @Site    : 
# @File    : StreamJobConf.py
# @Software: PyCharm

import json,base64,random,string

import TableGroupConf
from TableGroupTest import *

#创建随机名字的tablegroup，但targetprn必须是已存在的数据组，因此必须先调用API创建相同名字的tablegroup
randomstr = ''.join(random.sample(string.ascii_lowercase, 8))

#总的定义streamjob创建的post body
#定义streamjob的数据表生成间隔、资源配置
# Schedule,Config字典类型即可
Schedule = {"interval":30,"intervalUnit":"MINUTE"}
Config = {"cpu":'1000m',"mem":"500M"}
TopicName = "sddtopic0625"
StorageFormat = "json"

#读取streamjob的schema说明JSON文件内容到字典里
with open("/Users/sundiandong/sunwork/Pywork/kafka_test/StreamJob_conf", 'r') as f:
    TransactionSchema = json.loads(f.read())

#将字典内容转成JSON字符串
TransactionSchema = json.dumps(TransactionSchema,sort_keys=True,indent=4,separators=(',',':'))

#将JSON字符串进行BASE64编码
TransactionSchema = base64.standard_b64encode(TransactionSchema)

#TargetPrn 与 TableGroupPrn的 名字段 是一致的
TargetPrn = TableGroupConf.PostBodyDic["prn"]
JobType = "STREAM_KAFKA_TO_HDFS"
Prn = TableGroupConf.PostBodyDic["prn"].replace("table-group","stream-job")
# Prn = "sddnamespace/" + randomstr + ".stream-job"
KafkaConnect = "172.27.10.20:9093"
