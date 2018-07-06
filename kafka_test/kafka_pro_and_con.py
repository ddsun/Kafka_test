# -*- coding: utf-8 -*-
import pykafka
from pykafka import KafkaClient
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json,string,random
from kafka.structs import TopicPartition

class Kafka_producer():
    '''
    使用kafka的生产模块
    '''

    def __init__(self, kafkahost,kafkaport, kafkatopic):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.producer = KafkaProducer(bootstrap_servers = '{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafkaHost,
            kafka_port=self.kafkaPort
            ))

    def sendjsondata(self, params):
        try:
            parmas_message = json.dumps(params)
            print parmas_message
            producer = self.producer
            producer.send(self.kafkatopic, parmas_message.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print e

    def senddata(self, params):
        try:
            producer = self.producer
            # print params
            producer.send(self.kafkatopic, params.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print e

    def close(self):
        producer = self.producer
        producer.close(1000)

class Kafka_consumer():
    '''
    使用Kafka—python的消费模块
    '''

    def __init__(self, kafkahost, kafkaport, kafkatopic):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        # self.groupid = groupid
        self.consumer = KafkaConsumer(self.kafkatopic,bootstrap_servers = '{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafkaHost,
            kafka_port=self.kafkaPort
        ))

    def consume_data(self):
        try:
            for message in self.consumer:
                # print json.loads(message.value)
                yield message
        except KeyboardInterrupt, e:
            print e
