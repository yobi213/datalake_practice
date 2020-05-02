import sys
import os
import json
import time
import csv
import multiprocessing
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime
import twitter_config
import pykafka
from nltk.tokenize import word_tokenize
from http.client import IncompleteRead

class TweetListener(StreamListener):
	def __init__(self,searchword,category,time_limit=180):
		self.start_time = time.time()
		self.limit = time_limit
		self.client = pykafka.KafkaClient("localhost:9092")
		self.producer = self.client.topics[bytes('twitter','ascii')].get_producer()
		self.keyword = searchword
		self.category = category
	def on_data(self, data):
		if (time.time() - self.start_time) < self.limit:
			dict_data = json.loads(data)
			# get full text if it exists
			if 'retweeted_status' in dict_data.keys() and 'extended_tweet' in dict_data["retweeted_status"].keys() and "full_text" in dict_data['retweeted_status']['extended_tweet'].keys():
				text = dict_data["retweeted_status"]['extended_tweet']['full_text']
			elif 'text' in dict_data.keys():
				text = dict_data['text']
			else:
				text = None
			dict_data['text'] = text
			dict_data['keyword'] = self.keyword
			dict_data['category'] = self.category
			print("text : ", dict_data['text'], "keyword : ", dict_data['keyword'])
			self.producer.produce(bytes(json.dumps(dict_data), 'ascii'))
			return True
		else:
			print("timeout")
			sys.exit()
			return False




	def on_error(self, status):
		print(status)


if __name__ == "__main__":
        consumer_key = twitter_config.consumer_key
        consumer_secret = twitter_config.consumer_secret
        access_token = twitter_config.access_token
        access_secret = twitter_config.access_secret

        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)

        if len(sys.argv) < 2:
                print("Usage: PYSPARK_PYTHON=python3 /bin/spark-submit ex.py <YOUR WORD>", file=sys.stderr)
                exit(-1)

        path = '~/'
        os.chdir(path)
        filename = sys.argv[1]
        with open(filename,newline='') as f:
                reader = list(csv.reader(f))
                keywords = reader[0]
        print(keywords)

        def get_stream(keyword):
                word = list()
                word.append(keyword)
                print(word)
                twitter_stream = Stream(auth, TweetListener(keyword,filename))
                twitter_stream.filter(languages=['en'], track=word)

        pool = multiprocessing.Pool(processes=6)
        pool.map(get_stream, keywords)
        pool.close()
        pool.join()                      
