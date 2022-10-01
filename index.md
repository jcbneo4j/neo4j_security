---
layout: page
title: Neo4j Spark Connector
tagline: Using the Neo4j Spark Connector

description: Using the Neo4j Spark Connector
---

The [Neo4j Spark Connector Site](https://neo4j.com/docs/spark/current/) is a plugin available from Neo4j to enable a developer to perform read/writes to a Neo4j database from within a Spark environment. In this post we'll give an example of using the connector to pull data from parquet files hosted in AWS S3 and work within a notebook used in the [Amazon EMR](https://neo4j.com/docs/spark/current/) Spark environment. 

In a first high-level example (assuming the Neo4j spark connector is already configured in our environment), we perform the following:

    - create connection to the spark connector
    - build a sample set of data in a simple representation of people with their data of birth and write to a spark dataframe
    - we take the dataframe and write to a parquet file (on disk in this example)
    - read the parquet files into a personDF dataframe variable
    - take the personDF dataframe calling the .write method to pass in the Neo4j database configuration of the 'peopletest' database (previously configured) - note the 'query' .option that passes in cypher to create the Person nodes. Specifically, the pattern of the event, which is the variable pointing to the active record being read as a batch (as an UNWIND operation 'under the hood' with the connector). For further details it is referenced [here](https://neo4j.com/docs/spark/current/writing/#write-query)
    - after the data is written to the database, the next step is to perform the spark.read on the 'peopletest' database and render the details from the Person nodes - in our case, only the ids and dob of the people.
    
```python
import pyspark

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("persons").getOrCreate()

#build representation of people data
data =[("123 ","12/01/2000","John Smith"),
       ("222 ","03/21/1972","Jane Doe"),
       ("333 ","02/07/1987","Harry Jones"),
       ("444 ","11/11/1996","Erica Gomez"),
       ("555 ","07/04/2004","Brian Lee")]

columns=["id","date_of_birth","name"]
df=spark.createDataFrame(data,columns)

#write to parquet file
df.write.mode("overwrite").parquet("/tmp/output/people.parquet")

#read from parquet file (could run sql, etc.)
peopleDF=spark.read.parquet("/tmp/output/people.parquet")
peopleDF.createOrReplaceTempView("peopleTemp")
#peopleDF.printSchema()

#show data from parquet
print("Input from parquet files:\n")
peopleDF.show(truncate=False)

#write people data to neo4j db as Person nodes with 
peopleDF.write \
    .format("org.neo4j.spark.DataSource") \
    .mode("Overwrite") \
    .option("url", "bolt://localhost:7687") \
    .option("authentication.type", "basic") \
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "password") \
    .option("database", "peopletest") \
    .option("query", "CREATE (n:Person {id: event.id, dob: event.date_of_birth})") \
    .save()

#read Person nodes from neo4j
print("Reading newly created nodes in neo4j:\n")
spark.read.format("org.neo4j.spark.DataSource") \
    .option("url", "bolt://localhost:7687") \
    .option("authentication.type", "basic") \
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "password") \
    .option("database", "peopletest") \
    .option("labels", "Person") \
    .load().show()


Input from parquet files:

+------------+-------------+---------------------+
|id          |date_of_birth|description          |
+------------+-------------+---------------------+
|222         |03/21/1972   |Jane Doe             |
|555         |07/04/2004   |Brian Lee            |
|444         |11/11/1996   |Erica Gomez          |
|123         |12/01/2000   |John Smith           |
|333         |02/07/1987   |Harry Jones          |
+------------+-------------+---------------------+

Reading newly created nodes in neo4j:

+----+--------+-----------+-----------+
|<id>|<labels>|dob        |id         |
+----+--------+-----------+-----------+
|   0| [Person]|02/07/1987|       333 |
|   1| [Person]|03/21/1972|       222 |
|   2| [Person]|11/11/1996|       444 |
|   3| [Person]|12/01/2000|       123 |
|   4| [Person]|07/04/2004|       555 |
+----+--------+-----------+-----------+


```

The code snippets used below reference a Jupyter [notebook](https://github.com/jcbneo4j/neo4j_spark_parquet) containing the full code that can be used as a template as a starting point to implement using the connector. One could import the notebook into an Amazon EMR studio, point to their data and configure, as needed. 


```python
%%configure -f
{"driverMemory": "32000M","executorCores":15,
"conf": {
    "spark.jars": "s3:/<s3_bucket_name>/neo4j/neo4j-connector-apache-spark_2.11-4.1.2_for_spark_2.4.jar"
}
}
```







