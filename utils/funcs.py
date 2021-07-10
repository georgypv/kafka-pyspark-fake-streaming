import random
import yaml
import time
from uuid import uuid4

from faker.providers import BaseProvider

from pyspark.sql.types import *
from pyspark.sql import functions as f

with open('config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

URL = cfg['mysql']['url']
DBTABLE = cfg['mysql']['dbtable']
USER = cfg['mysql']['user']
PSWRD = cfg['mysql']['password']


def write_to_mysql(df, epoch_id, url=URL, dbtable=DBTABLE, user=USER, password=PSWRD):
    df.withColumn('batchId', f.lit(epoch_id))\
        .write.format('jdbc')\
        .option('url', url)\
        .option('dbtable', dbtable)\
        .option('driver', 'com.mysql.cj.jdbc.Driver')\
        .option('user', user)\
        .option('password', password)\
        .mode('append')\
        .save()


def df_preprocess(df):

    json_schema = StructType([
        StructField('order_uuid', StringType()),
        StructField('cafe_name', StringType()),
        StructField('first_name', StringType()),
        StructField('last_name', StringType()),
        StructField('address', StringType()),
        StructField('phone_number', StringType()),
        StructField('cc_number', StringType()),
        StructField('cc_expire', StringType()),
        StructField('dishes', ArrayType(StringType())),
        StructField('beverages', ArrayType(StringType())),
    ])

    df_preprocessed = df.select(df.value.cast("string").alias('info_json'), df.offset, df.timestamp)\
                        .withColumn('parsed_json', f.from_json('info_json', json_schema))\
                        .select('parsed_json.*', 'offset', 'timestamp')\
                        .withColumn('dishes', f.array_join(f.col('dishes'), '|'))\
                        .withColumn('beverages', f.array_join(f.col('beverages'), '|'))\
                        .withColumnRenamed('offset', 'kafka_offset')\
                        .withColumnRenamed('timestamp', 'kafka_timestamp')

    return df_preprocessed

class CafeProvider(BaseProvider):

    def cafe_name(self):
        cafe_names = ['Bolshaya St.',
                      'Ivanovskaya St.',
                      'Airport']
        return cafe_names[random.randint(0, len(cafe_names) - 1)]

    def dish_name(self):
        dish_names = ['Continental breakfast',
                      'Berlin waffles',
                      'Five Stars Exculsive',
                      'Big Eye omlette',
                      'French fries',
                      'Warsaw pudding',
                      'Strawberry cake']

        return dish_names[random.randint(0, len(dish_names) - 1)]

    def beverage_name(self):
        beverages = ['americano',
                     'cappuccino',
                     'water',
                     'fruit juice',
                     ]

        return beverages[random.randint(0, len(beverages) - 1)]


def get_order_info(fake, max_dishes_num, max_beverage_num):
    cafe = fake.cafe_name()

    dishes_lst = []
    dishes_num = random.randint(1, max_dishes_num)
    for _ in range(dishes_num):
        dishes_lst.append(fake.dish_name())

    bvrg_lst = []
    bvrg_num = random.randint(1, max_beverage_num)
    for _ in range(bvrg_num):
        bvrg_lst.append(fake.beverage_name())

    msg = {
        'order_uuid': str(uuid4()),
        'cafe_name': cafe,
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'address': fake.address(),
        'phone_number': fake.phone_number(),
        'cc_number': fake.credit_card_number(),
        'cc_expire': fake.credit_card_expire(),
        'dishes': dishes_lst,
        'beverages': bvrg_lst
    }

    key = {'cafe_name': cafe}

    return msg, key


def run_fake_stream(producer, fake, N, topic_name, max_dishes_num, max_beverage_num, verbose=False):
    for i in range(N):
        msg, key = get_order_info(fake, max_dishes_num, max_beverage_num)
        if verbose:
            print(msg)
        sleep_time = random.random()
        time.sleep(sleep_time + 1)

        producer.send(topic_name, key=key, value=msg)

        if i % 100 == 0:
            producer.flush()
    producer.flush()
    producer.close()
