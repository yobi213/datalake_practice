from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import sys
from pyspark.sql.types import *
import pandas as pd
#pandas_udf : spark 2.3
from pyspark.sql.functions import pandas_udf, PandasUDFType
import keras
from nltk import word_tokenize
imdb = keras.datasets.imdb

model = keras.models.load_model('sentiment_model.h5')
word_index = imdb.get_word_index()

# The first indices are reserved
word_index = {k:(v+3) for k,v in word_index.items()} 
word_index["<PAD>"] = 0
word_index["<START>"] = 1
word_index["<UNK>"] = 2  # unknown
word_index["<UNUSED>"] = 3
def get_sentiment(text):
	sentiment = []
	for t in text:
		test = []
		t = str(t)
		vect_text = t.lower()
		for word in word_tokenize(vect_text):
			if word not in word_index or word_index[word] > 20000:
				word_index[word] = 2
			test.append(word_index[word])
		test=keras.preprocessing.sequence.pad_sequences([test],value = word_index["<PAD>"],padding = 'post',maxlen=256)
		prediction = model.predict(test)
		if prediction >= 0.5:
			sentiment.append("positive")
		else: sentiment.append("negative")
	return pd.Series(sentiment)

if __name__ == "__main__":

	schema = StructType([                                                                                          
		StructField("text", StringType(), True),
		StructField("keyword", StringType(), True),
		StructField("category", StringType(), True),
		StructField('created_at', StringType(), True)   
	])
    
	spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
	spark.conf.set("spark.sql.execution.arrow.enabled", "true")
	kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "twitter").load()
	
	kafka_df_string = kafka_df.selectExpr("CAST(value AS STRING)")
	
	sdf = kafka_df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")
	pudf_sentiment = pandas_udf(get_sentiment, StringType())
	sdf2 = sdf.withColumn('sentiment', pudf_sentiment(col('text')))
	df = sdf2.withColumn('created_at', to_timestamp(sdf2.created_at, 'EEE MMM dd k:m:s z yyyy'))

	#df = sdf2.withColumn('created_at',from_unixtime(unix_timestamp("created_at",'EEE MMM dd k:m:s z yyyy')).cast("timestamp"))
	#query = df.writeStream.format("console").start()
	####save to elasticsearch : spark 2.2
	#query = df.writeStream.option("es.nodes","172.16.164.230").option("checkpointLocation","textlake/ES_checkpoint").format("org.elasticsearch.spark.sql").start("twitter/doc")
	query = df.writeStream.option("es.nodes","172.16.164.230").option("checkpointLocation","textlake/ES_checkpoint").format("org.elasticsearch.spark.sql").trigger(continuous="1 seconds").start("twitter/doc")

	query.awaitTermination()

