# Twtter - Kafka - Elasticsearch(HDFS) - Kibana

# kafka

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

+ add more broker
bin/kafka-server-start.sh config/server-1.properties &
bin/kafka-server-start.sh config/server-2.properties &


# structured streaming  ->  elasticsearch
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0,org.elasticsearch:elasticsearch-hadoop:6.4.3,org.apache.spark:spark-sql_2.11:2.3.0 econsumer.py

# structured streaming -> hdfs
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1 hconsumer.py

# hdfs -> elasticsearch 
spark-submit --packages org.elasticsearch:elasticsearch-hadoop:6.4.3 hdfs_es.py



# python3 tproducer.py <filename>
