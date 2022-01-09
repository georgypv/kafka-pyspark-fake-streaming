# -*- coding: utf-8 -*-
import json
import yaml
import sys


with open('config.yml', 'r', encoding='utf-8') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

from kafka import KafkaProducer
from faker import Faker

from utils import funcs
from fake_order.fake_order import OrderProvider

KAFKA_HOST = cfg['kafka']['kafka_host']
KAFKA_TOPIC = cfg['kafka']['topic_name']
N = int(sys.argv[1])
VERBOSE = bool(int(sys.argv[2]))

fake = Faker('RU')
fake.add_provider(OrderProvider)

producer = KafkaProducer(bootstrap_servers=KAFKA_HOST,
                         key_serializer=lambda x: json.dumps(x).encode('utf-8'),
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

try:
    funcs.run_fake_stream(producer,
                          fake,
                          N,
                          KAFKA_TOPIC,
                          verbose=VERBOSE)
except KeyboardInterrupt:
    print("Keyboard Interrupt! Closing Kafka Producer")
    producer.close()