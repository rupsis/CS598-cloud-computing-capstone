import json
import boto3
import os
import sys
import csv
import codecs
from kafka import KafkaProducer
from kafka.errors import KafkaError

bucket = 'cs598task1'
s3 = boto3.resource(u's3') 
bucket = s3.Bucket(u'cs598task1')

producer = KafkaProducer(
    bootstrap_servers=['b-1.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092', 'b-2.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def sendEvent(item):
    data = {}
    data['Year'] = item[0]
    data['Month'] = item[2]
    data['DayofMonth'] = item[3]
    data['CRSDepTime'] = item[23]
    data['ArrDelay'] = item[36]
    data['Carrier'] = item[8]
    data['Origin'] = item[11]
    data['Dest'] = item[17]

    print(data)
    producer.send('cs598-task2', data)


for i in range(1989, 2009):
    obj = bucket.Object(key=u'{}_clean.csv'.format(i))
    response = obj.get()
    lines = response[u'Body']

    for ln in codecs.getreader('utf-8')(lines):
        line = ln.split(',')
        if(line[0] != 'Year'):
            sendEvent(line)      