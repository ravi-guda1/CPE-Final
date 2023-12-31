from kafka import  KafkaProducer
from json import dumps
import csv

topicname='retailupdate'
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda x:dumps(x).encode('utf-8'))
with open("C:\Retail_result.csv",'r') as file:
 reader = csv.reader(file)
 for messages in reader:
  producer.send(topicname,messages)
  producer.flush()