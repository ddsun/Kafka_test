# -*- coding: utf-8 -*-
from datetime import datetime
import time

from kafka_pro_and_con import *

# HOST = "172.27.128.32"
# PORT = 9092
HOST = "172.27.10.20"
PORT = 9093
BOOTSTRAP_SERVER = HOST + ":" + str(PORT)
TOPIC = "sddtopic0630_3"
GROUP_ID = "test0630_3"
def producer_test():
    count = 0
    # 测试生产模块
    producer = Kafka_producer(HOST,PORT,TOPIC)
    # for id1 in range(1, 2):

    try:
        # while True:
        for i in range(1,2):
            dt = datetime.now()
            id = random.randint(1, 999999)
            age = random.randint(1, 50)
            namestr = ''.join(random.sample(string.ascii_lowercase, 8))

            params = \
                {
                    "user": id,
                    "age":age,
                    "sex":u"男",
                    "record_time":dt.strftime('%Y-%m-%d %H:%M:%S')
                }
            producer.sendjsondata(params)
            # print i
            # params = "onemessage" + str(i) + "_" + namestr + "," + str(age) + "," + u"男" + "," + dt.strftime('%Y-%m-%d %H:%M:%S') #+ "\n"
            # with open("/Users/sundiandong/sunwork/Pywork/kafka_test/param.json", 'r') as f:
            #     params = json.loads(f.read())

            # producer.senddata(params)
            print params
            count += 1
    except KeyboardInterrupt, e:
        print e
    producer.close()
    return count


def producer_test_csv():
    count = 0
    producer = Kafka_producer(HOST, PORT, TOPIC)
    try:
        # while True:
        for i in range(1, 20001):
            dt = datetime.now()
            age = random.randint(1, 50)
            namestr = ''.join(random.sample(string.ascii_lowercase, 20))

            params = namestr + "," + str(age) + "," + u"男" + "," + "2018-06-29 23:30:30\n" \
                     + namestr + "," + str(age) + "," + u"女" + "," + "2018-06-29 23:34:30\n" \
                     + "LALALA" + "," + str(age) + "," + u"男" + "," + dt.strftime('%Y-%m-%d %H:%M:%S')
            # params = str(i) + "_" + namestr + "," + str(age) + "," + u"男" + "," + dt.strftime('%Y-%m-%d %H:%M:%S') + "\n"
            print params
            # with open("/Users/sundiandong/sunwork/Pywork/kafka_test/param.json", 'r') as f:
            #     params = json.loads(f.read())
            producer.senddata(params)
            count += 1
    except KeyboardInterrupt, e:
        print e
    producer.close()
    return count

#通过传参生产消息
def producer_params_test(params):
    count = 0
    producer = Kafka_producer(HOST, PORT, TOPIC)
    try:
        # while True:
        for i in range(1, 11):
            # age = random.randint(1, 50)
            # namestr = ''.join(random.sample(string.ascii_lowercase, 20))
            #
            # params = str(i) + "_" + namestr + "," + str(age) + "," + u"男" + "," + dt.strftime('%Y-%m-%d %H:%M:%S') + "\n"
            print params
            producer.senddata(params)
            count += 1
    except KeyboardInterrupt, e:
        print e
    producer.close()
    return count

#自定义列数生产消息
def producer_define_colnum(colnum,msgcount):
    producer = Kafka_producer(HOST, PORT, TOPIC)
    for ii in range(msgcount):
        msg = ""
        strlist = []
        for i in range(colnum):
            tmpstr = ''.join(random.sample(string.ascii_lowercase, 8))
            strlist.append(tmpstr)
        msg = ','.join(strlist)
        print msg
        try:
            producer.senddata(msg)
        except KeyboardInterrupt, e:
            print e
    producer.close()


def producer_mix_msg():
    producer = Kafka_producer(HOST, PORT, TOPIC)
    count_total = 0
    count_correct = 0
    try:
        for i in range(1, 100001):
            dt = datetime.now()
            age = random.randint(1, 50)
            namestr = ''.join(random.sample(string.ascii_lowercase, 20))

            # params = namestr + "," + str(age) + "," + u"男" + "," + "2018-06-29 23:30:30\n" \
            #          + namestr + "," + str(age) + "," + u"女" + "," + "2018-06-29 23:34:30\n" \
            #          + "LALALA" + "," + str(age) + "," + u"男" + "," + dt.strftime('%Y-%m-%d %H:%M:%S')
            params_csv = str(i) + "_" + namestr + "," + str(age) + "," + u"男" + "," + dt.strftime('%Y-%m-%d %H:%M:%S')
            params_json = {
                    "user":''.join(random.sample(string.ascii_lowercase, 8)),
                    "age":age,
                    "sex":u"男",
                    "record_time":dt.strftime('%Y-%m-%d %H:%M:%S')
                }
            params_error = "Error message!!!"
            params_many_rds = namestr + "," + str(age) + "," + u"男" + "," + "2018-06-29 23:30:30\n" \
                     + namestr + "," + str(age) + "," + u"女" + "," + "2018-06-29 23:34:30\n" \
                     + "LALALA" + "," + str(age) + "," + u"男" + "," + dt.strftime('%Y-%m-%d %H:%M:%S')

            print "CSV Message:\n" + params_csv
            print "JSON Message: "
            print params_json
            print "Error Message:\n" + params_error
            print "Many Records Message:\n" + params_many_rds

            producer.senddata(params_csv)
            # json消息发送这里不用打印，sendjsondata函数里面有打印
            # producer.sendjsondata(params_json)
            producer.senddata(params_error)
            producer.senddata(params_many_rds)
            count_total += 6
            count_correct += 4
    except KeyboardInterrupt, e:
        print e
    print "\nCSV格式Message"
    print "共发送%d条records" % count_total
    print "正确格式%d条records" % count_correct
    producer.close()

def consumer_test():
    # auto_offset_reset:重置偏移量，earliest移到最早的可用消息，latest最新的消息，默认为latest
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[BOOTSTRAP_SERVER],group_id=GROUP_ID, auto_offset_reset='earliest',enable_auto_commit=True)
    # print consumer.partitions_for_topic(TOPIC)  # 获取sdd0622主题的分区信息
    # print consumer.topics()  # 获取主题列表
    # print consumer.subscription()  # 获取当前消费者订阅的主题
    # print consumer.assignment()  # 获取当前消费者topic、分区信息
    # print consumer.beginning_offsets(consumer.assignment())  # 获取当前消费者可消费的偏移量
    # consumer.seek(TopicPartition(topic='sddtopic1', partition=1), 5)  # 重置偏移量，从第5个偏移量消费
    print "\n\n"
    # 消费模块的返回格式为ConsumerRecord(topic=u'ranktest', partition=0, offset=202, timestamp=None,
    # \timestamp_type=None, key=None, value='"{abetst}:{null}---0"', checksum=-1868164195,
    # \serialized_key_size=-1, serialized_value_size=21)
    count = 0
    try:
        for message in consumer:
            count += 1
            print message.value
            print count
    except KeyboardInterrupt, e:
        print e

    # print consumer.beginning_offsets(consumer.assignment())
    consumer.close(autocommit=True)


if __name__ == '__main__':
    # producer_define_colnum(colnum=100,msgcount=5)
    time_start = time.time()
    # count = producer_test_csv()
    producer_mix_msg()
    # count1 = producer_test()
    time_end = time.time()
    total_time = (int(round(time_end * 1000))) - (int(round(time_start * 1000)))
    # print "生产%d条消息，用时 %sms\n" % (count+count1,total_time)
    # consumer_test()