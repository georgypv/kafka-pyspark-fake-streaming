# -*- coding: utf-8 -*-
import yaml
import time
from uuid import uuid4
import random

from pyspark.sql import functions as f

with open('config.yml', 'r', encoding='utf-8') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

URL = cfg['mysql']['url']
DBTABLE = cfg['mysql']['dbtable']
USER = cfg['mysql']['user']
PSWRD = cfg['mysql']['password']


##### FUNCTIONS FOR PRODUCER ###########
#generate single message with order info
def get_order(fake, max_books_in_order=10):
    num_books= random.randint(1,max_books_in_order)

    book_categories = [fake.book_category() for  _ in range(num_books)]
    book_formats = [fake.book_format() for _ in range(num_books)]
    book_ratings = [fake.book_rating() for _ in range(num_books)]

    assert len(book_categories) == len(book_formats) == len(book_ratings)

    msg = {
        'order_uuid': str(uuid4()),
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'address': fake.address(),
        'phone_number': fake.phone_number(),
        'cc_number': fake.credit_card_number(),
        'cc_expire': fake.credit_card_expire(),
        'book_category': book_categories,
        'book_format': book_formats,
        'book_rating':book_ratings,
    }
    return msg

#generate N messages
def run_fake_stream(producer, fake, N, topic_name, verbose=False):
    for i in range(N):
        msg = get_order(fake)
        if verbose:
            print(msg)
        sleep_time = random.random()
        time.sleep(sleep_time*0.01)

        producer.send(topic_name, value=msg)

        if i % 100 == 0:
            producer.flush()
    producer.flush()
    producer.close()


###### WRITE SPARK STREAM TO MYSQL #############
def write_to_mysql(df, epoch_id, url=URL, dbtable=DBTABLE, user=USER, password=PSWRD):
    df.withColumn('batch_id', f.lit(epoch_id))\
        .write.format('jdbc')\
        .option('url', url)\
        .option('dbtable', dbtable)\
        .option('driver', 'com.mysql.cj.jdbc.Driver')\
        .option('user', user)\
        .option('password', password)\
        .mode('append')\
        .save()
