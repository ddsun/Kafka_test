#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/23 下午2:13
# @Author  : Sundiandong
# @Site    : 
# @File    : kafka_thead.py
# @Software: PyCharm

import threading
import time
from kafka_test import *

THREAD_COUNT = 20
class MyThread(threading.Thread):
    def __init__(self,n):
        super(MyThread,self).__init__()
        self.n=n
    def run(self):
        producer_mix_msg()
        print("producer ",self.n)

class ParamsProducer(threading.Thread):
    def __init__(self,n):
        super(ParamsProducer,self).__init__()
        self.n=n
    def run(self):
        dt = datetime.now()
        age = random.randint(1, 50)
        namestr = ''.join(random.sample(string.ascii_lowercase, 20))
        params = "SDD" + "_" + namestr + "," + str(age) + "," + u"男" + "," + dt.strftime('%Y-%m-%d %H:%M:%S') + "\n"
        producer_params_test(params)
        print("producer ",self.n)

def StartThreadNum(n):
    for i in range(n):
        try:
            # th = MyThread(i)
            th = MyThread(i)
            th.start()
        except KeyboardInterrupt, e:
            print e

if __name__ == '__main__':
    StartThreadNum(THREAD_COUNT)
