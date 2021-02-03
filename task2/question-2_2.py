from __future__ import print_function

# bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.1  --executor-memory 20G --master yarn --num-executors 50  ~/task2/question-2_1.py 

import sys
import uuid
import json
import boto3
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def save_results(items):
    dynamodb = boto3.resource('dynamodb', region_name="us-east-1")

    table = dynamodb.Table('cs598_question_2_2')
    for item in items:
        origin = item['origin']
        dest = item['dest']
        avg_depdelay = item['avg_depdelay'] 
        table.put_item(Item=item)

def saveTopCarriers(carriers):
    sorted = carriers.sortByKey(True)
    for toSave in sorted.take(10):
        save_results(toSave)


def calculateAverage(newVal, accumlativeAvg):
    if accumlativeAvg is None:
        accumlativeAvg = (0.0, 0, 0.0)
    total = sum(newVal, accumlativeAvg[0])
    count = accumlativeAvg[1] + len(newVal)
    avg = total / float(count)
    return (prod, count, avg)


if __name__ == '__main__':
    conf = SparkConf()
    conf.setAppName("Problem_2-2")
    conf.set("spark.streaming.kafka.maxRatePerPartition", 50000)
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.python.worker.memory", "1g")

    airports = ['CMI', 'BWI', 'MIA', 'LAX', 'IAH', 'SFO']

    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 2)
    ssc.checkpoint("/tmp/streaming")

    brokers = "b-1.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092,b-2.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092"
    topic = "cs598-task2 "
    kafka_consumer_group = str(uuid.uuid4())
    kafka_client_params = {
        "metadata.broker.list": brokers,
        "group.id": kafka_consumer_group,
        "auto.offset.reset": "smallest",
    }

    events = KafkaUtils.createDirectStream(ssc, [topic], kafka_client_params)
    parsed = events.map(lambda line: json.loads(line[1]))

    # Filter based on airport list
    filtered_airports = parsed.filter(lambda item: item['Dest'] in airports)

    # compute averages 
    filtered_airports = filtered_airports.updateStateByKey(calculateAverage)

    # for each airport, save to ten items
    for airport in airports:
        saveTopCarriers(filtered_airports.filter(lambda item: item['Dest'] == airport))

    print("Completed")

    ssc.start()
    ssc.awaitTermination()