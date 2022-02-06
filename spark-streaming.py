# -*- coding: utf-8 -*-
import os
import sys
from builtins import Exception, KeyboardInterrupt

import yaml

with open('config.yml', 'r', encoding='utf-8') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
print(cfg)

SPARK_HOME = cfg['spark']['spark_home']
PYSPARK_SUBMIT_ARGS = cfg['spark']['pyspark_submit_args']
JAVA_HOME = cfg['spark']['java_home']
KAFKA_HOST = cfg['kafka']['kafka_host']
KAFKA_TOPIC = cfg['kafka']['topic_name']
KAFKA_STARTING_OFFSET = cfg['kafka']['starting_offset']

DBTABLE_AGG = cfg['mysql']['dbtable_agg']

os.environ['SPARK_HOME'] = SPARK_HOME
os.environ['PYSPARK_SUBMIT_ARGS'] = PYSPARK_SUBMIT_ARGS
os.environ['JAVA_HOME'] = JAVA_HOME
sys.path.insert(1, os.path.join(SPARK_HOME, 'python'))
sys.path.insert(1, os.path.join(SPARK_HOME, 'python', 'pyspark'))
sys.path.insert(1, os.path.join(SPARK_HOME, 'python', 'build'))

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f

from functools import partial
from utils import funcs

write_to_mysql_agg = partial(funcs.write_to_mysql, dbtable=DBTABLE_AGG)


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
        StructField('book_rating', ArrayType(StringType())),
    ])

    df_preprocessed = df.select(df.value.cast("string").alias('info_json'), df.offset, df.timestamp) \
        .withColumn('parsed_json', f.from_json('info_json', json_schema)) \
        .select('parsed_json.*', 'offset', 'timestamp') \
        .withColumn('book_category', f.array_join(f.col('book_category'), '|')) \
        .withColumn('book_format', f.array_join(f.col('book_format'), '|')) \
        .withColumn('book_rating', f.array_join(f.col('book_rating'), '|')) \
        .withColumnRenamed('offset', 'kafka_offset') \
        .withColumnRenamed('timestamp', 'kafka_timestamp')

    return df_preprocessed


try:
    print('Starting a Spark Session')
    spark = SparkSession\
        .builder\
        .master('local') \
        .appName('Fake Orders Stream')\
        .config('spark.executorEnv.JAVA_HOME', JAVA_HOME)\
        .config('spark.sql.streaming.forceDeleteTempCheckpointLocation', 'true')\
        .config('spark.sql.shuffle.partitions', 4)\
        .getOrCreate()
    print('Spark Session is initiated')
    print("SPARK UI URL:", spark.sparkContext.uiWebUrl)
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

    query_agg = order_info\
        .withWatermark('kafka_timestamp', '2 minutes')\
        .groupBy(f.window('kafka_timestamp', '1 minute').alias('minute_window'))\
        .agg(f.count('order_uuid').alias('order_cnt'))\
        .select(
            f.concat(
                f.col('minute_window')['start'].cast('string'),
                f.lit('<->'),
                f.col('minute_window')['end'].cast('string')
                ).alias('time_window'),
                'order_cnt')\
        .writeStream\
        .queryName('Fake-Stream-Time-Windows')\
        .foreachBatch(write_to_mysql_agg)\
        .start()

    print(f'Writing aggregates is running: {query_agg.isActive}')

    query.awaitTermination()
    query_agg.awaitTermination()

except KeyboardInterrupt:
    print('Keyboard Interrupt: stopping stream and Spark Session')
    query.stop()
    query_agg.stop()
    spark.stop()


