---
layout: page
title: Neo4j Spark Connector
tagline: Using the Neo4j Spark Connector

description: Using the Neo4j Spark Connector
---

The [Neo4j Spark Connector Site](https://neo4j.com/docs/spark/current/) is a plugin available from Neo4j to enable a developer to perform read/writes to a Neo4j database from within a Spark environment. In this post we'll give an example of using the connector to pull data from parquet files hosted in AWS S3 and work within a notebook used in the [Amazon EMR](https://neo4j.com/docs/spark/current/) Spark environment. 

```python
%%configure -f
{"driverMemory": "32000M","executorCores":15,
"conf": {
    "spark.jars": "s3:/<s3_bucket_name>/neo4j/neo4j-connector-apache-spark_2.11-4.1.2_for_spark_2.4.jar"
}
}
```







