# Twtter - Kafka - Elasticsearch(HDFS) - Kibana



<div>
 <img width="664" alt="스크린샷 2020-05-02 오후 7 08 09" src="https://user-images.githubusercontent.com/39682914/80861263-654a8b80-8ca8-11ea-8933-b71272fb5d47.png">
 </div>

* KAFKA

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

* structured streaming  ->  elasticsearch

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0,org.elasticsearch:elasticsearch-hadoop:6.4.3,org.apache.spark:spark-sql_2.11:2.3.0 econsumer.py

* structured streaming -> hdfs

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1 hconsumer.py

* hdfs -> elasticsearch 

spark-submit --packages org.elasticsearch:elasticsearch-hadoop:6.4.3 hdfs_es.py

* twitter -> kafka

python3 tproducer.py <filename>


 
 # Kibana
 <div>
<img width="649" alt="스크린샷 2020-05-02 오후 7 06 20" src="https://user-images.githubusercontent.com/39682914/80861266-68de1280-8ca8-11ea-957c-ba6623f5a1c0.png">
</div>

 
 Paper : [실시간 스트리밍 텍스트 데이터 분석 시스템 설계](https://www.dbpia.co.kr/Journal/articleDetail?nodeId=NODE08003264)
