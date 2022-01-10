# Example of how to load generated data into a MySQL table with PySpark Streaming

Stucture of the project:
1) Generate fake flow of orders in a loop
2) Send order messages into a message broker (Apache Kafka)
3) Read messages with PySpark (Structured Streaming)
4) Stream them into a MySQL table


