# Twtter - Kafka - Elasticsearch(HDFS) - Kibana

* KAFKA

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

* structured streaming  ->  elasticsearch

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0,org.elasticsearch:elasticsearch-hadoop:6.4.3,org.apache.spark:spark-sql_2.11:2.3.0 econsumer.py

* structured streaming -> hdfs

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1 hconsumer.py

* hdfs -> elasticsearch 

spark-submit --packages org.elasticsearch:elasticsearch-hadoop:6.4.3 hdfs_es.py

* python

python3 tproducer.py <filename>
