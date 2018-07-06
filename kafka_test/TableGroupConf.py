#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/23 下午6:33
# @Author  : Sundiandong
# @Site    : 
# @File    : TableGroupConf.py
# @Software: PyCharm

import json,base64,random,string,time
#创建随机名字的tablegroup
randomstr = ''.join(random.sample(string.ascii_lowercase, 8))
#读取JSON文件默认内容到字典里
with open("/Users/sundiandong/sunwork/Pywork/kafka_test/TableGroup_conf","r") as f:
    PostBodyDic = json.loads(f.read())
#修改其中的关键字段
PostBodyDic["name"] = randomstr
PostBodyDic["prn"] = "sddnamespace/" + randomstr + ".table-group"
PostBodyDic["createTime"] = PostBodyDic["updateTime"] = int(round(time.time()) * 100)
PostBodyDic["tag"] = PostBodyDic["schema"] = None
#将字典内容转成JSON字符串
PostJson = json.dumps(PostBodyDic,sort_keys=True,indent=4,separators=(',',':'))