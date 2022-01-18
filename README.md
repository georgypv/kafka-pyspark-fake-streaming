# Example of how to load generated data into a MySQL table with PySpark Streaming

General concept of the project:
1) Generate fake flow of orders in a loop
2) Send order messages into a message broker (Apache Kafka)
3) Read messages with PySpark (Structured Streaming)
4) Stream them into a MySQL table

To replicate result locally, one should:

1) Change connection paths to MySQL DB, Kafka broker and paths to Spark and Java files in the `config.yml`
2) Create a SQL-table using script in `create-mysql-db.sql`
3) Create a topic in Apache Kafka (with the same name as in `config.yml`)
4) Run `produce-fake-stream.py` to generate stream of messages which will be sent into the Kafka topic.  `produce-fake-stream.py` has arguments: 1) number of messages and  2) verbose (1 - to print out every generated message, 0 - otherwise). For example: ```python produce-fake-stream.py 10000 1```
5) Separately run `spark-streaming.py` to read the stream of messages with PySpark and write it batch-wise into the MySQL table. 

