#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/27 下午5:13
# @Author  : Sundiandong
# @Site    : 
# @File    : MessageFromCsv.py
# @Software: PyCharm

import csv
import json
import datetime
import TableGroupConf
from TableGroupTest import *
from kafka_pro_and_con import *

FilePath = "/Users/sundiandong/sunwork/CSV_dataupload/pdms_data/shannon_csv"
# FIleName = "pdms_functiontest_multiLineFieldSeparator_10K.csv"
FIleName = "pdms_functiontest_base_10K.csv"
# FIleName = "pdms_functiontest_base_10K.csv"
HOST = "172.27.10.0"
PORT = 9093
BOOTSTRAP_SERVER = HOST + ":" + str(PORT)
TOPIC = "topic_real"
GROUP_ID = "test704_2"


'''
读取CSV文件行首信息并将其写入schema说明json文件,并返回此json
'''
def CSVtitle2Schema(filepath):
    schlist = []
    count = 0
    with open(filepath,'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            for i in row:
                dic = {"name": "", "type": "String"}
                dic["name"] = i
                schlist.append(dic)
            if count == 0:
                break
    # print schlist
    with open("/Users/sundiandong/sunwork/Pywork/kafka_test/StreamJob_conf",'r') as defaultschema:
        params = json.loads(defaultschema.read())
        params["fields"] = schlist
        params = json.dumps(params,sort_keys=True,indent=4,separators=(',',':'))
    return params

'''
参数pm传的是schema定义json文件，用于创建流式任务，暂时没用上
'''
def CSVcontent2Message(pm):
    count = 0
    msg = ""
    msgcount = 0
    # pm = base64.standard_b64encode(pm)
    producer = Kafka_producer(HOST, PORT, TOPIC)

    with open(filepath,'rb') as csvfile:
        reader = csv.reader(csvfile)
        # dt = datetime.now()
        try:
            for row in reader:
                # print row
                if count == 0:
                    count += 1
                    continue
                msg = ','.join(row)
                print msg
                # 这里decode一次是因为kafka_pro_and_con的senddata对消息进行了一次utf-8编码，因此这里先解码
                producer.senddata(msg.decode('utf-8'))
                msgcount += 1
        except KeyboardInterrupt, e:
            print e
        producer.close()
        return msgcount

#创建指定列数的schema
def CreateSchemaJson(colnum):
    schlist = []
    schemalist = ["column_" + str(elm) for elm in range(0,colnum)]
    for i in schemalist:
        dic = {"name": "", "type": "String"}
        dic["name"] = i
        schlist.append(dic)
    with open("/Users/sundiandong/sunwork/Pywork/kafka_test/StreamJob_conf",'r') as defaultschema:
        params = json.loads(defaultschema.read())
        params["fields"] = schlist
        params = json.dumps(params,sort_keys=True,indent=4,separators=(',',':'))
    print params

def CreateMessageAsSchema(colnum,msgcount):
    producer = Kafka_producer(HOST, PORT, TOPIC)
    for ii in range(msgcount):
        msglist = []
        msg = ""
        for i in range(colnum):
            msg = ''.join(random.sample(string.ascii_lowercase, 20))
            msglist.append(msg)
            msg = ','.join(msglist)
        print msg
        producer.senddata(msg.decode('utf-8'))


if __name__ == '__main__':
    # CreateSchemaJson(20)
    CreateMessageAsSchema(20,1)
    # filepath = FilePath + "/" + FIleName
    # schemajson = CSVtitle2Schema(filepath)
    # print schemajson
    # msgcount = CSVcontent2Message(schemajson)
    # print msgcount