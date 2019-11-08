from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

access_token = "2480068688-cBAp2P1Q3yhrFidgntBYk829hva54LFmXcFQpoz"
access_token_secret =  "BLCqbm8ebepPhwfE7v4l6n5zswOsbeFWNFrg9UJQF97Kg"
consumer_key =  "b5gNhsqS7St3E5kinj7fkt953"
consumer_secret =  "PhHuEpRapME55HtqrIbaFPO61Oh7kN4lYG3DMA86MByiqK0vfv"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("sk_trump", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="trump")
