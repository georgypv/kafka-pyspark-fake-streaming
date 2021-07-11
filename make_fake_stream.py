import json
import yaml
import sys

with open('config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

from kafka import KafkaProducer
from faker import Faker

from utils import funcs

KAFKA_HOST = cfg['kafka']['kafka_host']
KAFKA_TOPIC = cfg['kafka']['topic_name']
MAX_DISHES_NUM = cfg['faker']['max_dishes_num']
MAX_BEVERAGES_NUM = cfg['faker']['max_beverages_num']

N = int(sys.argv[1])
VERBOSE = bool(int(sys.argv[2]))

fake = Faker('RU')
fake.add_provider(funcs.CafeProvider)

producer = KafkaProducer(bootstrap_servers=KAFKA_HOST,
                         key_serializer=lambda x: json.dumps(x).encode('utf-8'),
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

try:
    funcs.run_fake_stream(producer,
                          fake,
                          N,
                          KAFKA_TOPIC,
                          MAX_DISHES_NUM,
                          MAX_BEVERAGES_NUM,
                          verbose=VERBOSE)
except KeyboardInterrupt:
    print("Keyboard Interrupt! Closing Kafka Producer")
    producer.close()
