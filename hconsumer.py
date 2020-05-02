from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import sys
from pyspark.sql.types import *
if __name__ == "__main__":
	spark = SparkSession.builder.appName("TwitterToHDFS").getOrCreate()
	kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "twitter").load()
	
	query = kafka_df.writeStream.option("path","textlake/twitter").option("checkpointLocation","textlake/checkpoint_twitter").start()
	####save to elasticsearch : spark 2.2
	#query = sdf.writeStream.option("es.nodes","172.16.164.230").option("checkpointLocation","/ES_checkpoint").format("org.elasticsearch.spark.sql").start("pudf/doc")
	query.awaitTermination()

