# -*- coding: utf-8 -*-
import os
from builtins import Exception, KeyboardInterrupt

import yaml

with open('config.yml', 'r', encoding='utf-8') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

print(cfg)

os.environ['SPARK_HOME'] = cfg['spark']['spark_home']
os.environ['PYSPARK_SUBMIT_ARGS'] = cfg['spark']['pyspark_submit_args']

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f

from utils import funcs

JAVA_HOME = cfg['spark']['java_home']
KAFKA_HOST = cfg['kafka']['kafka_host']
KAFKA_TOPIC = cfg['kafka']['topic_name']
KAFKA_STARTING_OFFSET = cfg['kafka']['starting_offset']


# preprocess kafka mesage in PySpark
def df_preprocess(df):
    json_schema = StructType([
        StructField('order_uuid', StringType()),
        StructField('first_name', StringType()),
        StructField('last_name', StringType()),
        StructField('address', StringType()),
        StructField('phone_number', StringType()),
        StructField('cc_number', StringType()),
        StructField('cc_expire', StringType()),
        StructField('book_category', ArrayType(StringType())),
        StructField('book_format', ArrayType(StringType())),
        StructField('book_rating', ArrayType(IntegerType())),
    ])

    df_preprocessed = df.select(df.value.cast("string").alias('info_json'), df.offset, df.timestamp) \
        .withColumn('parsed_json', f.from_json('info_json', json_schema)) \
        .select('parsed_json.*', 'offset', 'timestamp') \
        .withColumn('book_category', f.array_join(f.col('book_category'), '|')) \
        .withColumn('book_format', f.array_join(f.col('book_format'), '|')) \
        .withColumn('book_rating', df.book_rating.cast('array<string>')) \
        .withColumn('book_raing', f.array_join(f.col('book_rating'), '|')) \
        .withColumnRenamed('offset', 'kafka_offset') \
        .withColumnRenamed('timestamp', 'kafka_timestamp')

    return df_preprocessed


try:
    print('Starting a Spark Session')
    spark = SparkSession\
        .builder\
        .master('local') \
        .appName('Fake Stream')\
        .config('spark.executorEnv.JAVA_HOME', JAVA_HOME)\
        .getOrCreate()
    print('Spark Session is initiated')
except Exception as ex:
    print(str(ex))
    print('Exeption while starting a Spark Session')


print('Loading a Kafka stream')
msgs = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_HOST) \
    .option('subscribe', KAFKA_TOPIC) \
    .option("startingOffsets", KAFKA_STARTING_OFFSET) \
    .load()
print('Kafka Stream is loaded')


order_info = df_preprocess(msgs)
try:
    print('Starting to write output stream')
    query = order_info \
        .writeStream \
        .queryName('Fake-Stream') \
        .foreachBatch(funcs.write_to_mysql) \
        .start()
    print(f'Writing stream is running: {query.isActive}')
    query.awaitTermination()

except KeyboardInterrupt:
    print('Keyboard Interrupt: stopping stream and Spark Session')
    query.stop()
    spark.stop()


