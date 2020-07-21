## Kafka
```
# download Kafka
wget https://archive.apache.org/dist/kafka/2.2.1/kafka_2.11-2.2.1.tgz

tar -xzf kafka_2.11-2.2.1.tgz


# Creating a topic
bin/kafka-topics.sh --create --zookeeper z-1.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:2181,z-3.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:2181,z-2.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:2181 --replication-factor 1 --partitions 1 --topic test

bin/kafka-topics.sh --create --zookeeper b-1.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092,b-2.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092 --replication-factor 1 --partitions 1 --topic test

# Listing Topics 
 bin/kafka-topics.sh --list --zookeeper b-2.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092,b-1.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092

  bin/kafka-topics.sh --list --zookeeper z-1.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:2181,z-3.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:2181,z-2.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:2181


# Producer
bin/kafka-console-producer.sh --broker-list b-1.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092,b-2.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092 --topic test

#consumer
bin/kafka-console-consumer.sh --bootstrap-server b-1.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092,b-2.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092 --topic cs598-task2 --from-beginning


# allocate more memory to kafka:
KAFKA_HEAP_OPTS="-Xms1g -Xmx2g"

```