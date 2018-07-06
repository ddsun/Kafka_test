#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/23 下午7:37
# @Author  : Sundiandong
# @Site    : 
# @File    : MainTest.py
# @Software: PyCharm

import kafka_test
import kafka_pro_and_con
import kafka_thead
import StreamJobConf
from StreamJobTest import *
import TableGroupConf
from TableGroupTest import *

if __name__ == '__main__':
    tg = TableGroupTest()
    tg.TableGroupCreate()
    st = StreamJobTest()
    st.StreamJobCreate()
