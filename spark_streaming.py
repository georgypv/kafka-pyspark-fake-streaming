import os
import yaml

with open('config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

print(cfg)

os.environ['SPARK_HOME'] = cfg['spark']['spark_home']
os.environ['PYSPARK_SUBMIT_ARGS'] = cfg['spark']['pyspark_submit_args']

from pyspark.sql import SparkSession

from utils import funcs

JAVA_HOME = cfg['spark']['java_home']

KAFKA_HOST = cfg['kafka']['kafka_host']
KAFKA_TOPIC = cfg['kafka']['topic_name']
KAFKA_STARTING_OFFSET = cfg['kafka']['starting_offset']

print('Starting a Spark Session')
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('Fake Stream')\
    .config('spark.executorEnv.JAVA_HOME', JAVA_HOME)\
    .getOrCreate()
print('Spark Session is initiated')

print('Loading a Kafka stream')
msgs = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_HOST) \
    .option('subscribe', KAFKA_TOPIC) \
    .option("startingOffsets", KAFKA_STARTING_OFFSET) \
    .load()
print('Kafka Stream is loaded')


cafe_info = funcs.df_preprocess(msgs)

try:

    print('Starting to write an output stream')
    query = cafe_info \
        .writeStream \
        .queryName('Fake-Stream') \
        .foreachBatch(funcs.write_to_mysql) \
        .trigger(processingTime='5 seconds') \
        .start()
    query.awaitTermination()
    print(f'Writing stream is running: {query.isActive}')

except KeyboardInterrupt:
    print('Keyboard Interrupt: closing writing stream and stopping Spark Session')
    query.stop()
    spark.stop()


