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

The code snippets used below reference a Jupyter [notebook](https://github.com/jcbneo4j/neo4j_spark_parquet/blob/main/parquet_to_neo-generic.ipynb) containing the full code that can be used as a template as a starting point to implement using the connector. One could import the notebook into an Amazon EMR studio, point to their data and configure, as needed. 

In the first cell, the technique to load the neoj4 connector jar is to place in an S3 bucket and reference in the spark config, as follows. (NOTE: this is assumed it is running the notebook in an Amazon EMR Studio and that it would have the proper permissions to read the jar from the bucket):

```python
%%configure -f
{"driverMemory": "32000M","executorCores":15,
"conf": {
    "spark.jars": "s3:/<s3_bucket_name>/neo4j/neo4j-connector-apache-spark_2.11-4.1.2_for_spark_2.4.jar"
}
}
```

In the third cell we perform a spark.read from the S3 bucket where our parquet files are kept. Some basic Spark utility methods are shown as examples of performing a count in a dataframe, null checks, etc. 

The overall idea for this ETL is to read the data from the parquet files, perform any necessary transforms in Python/Spark and then write the data into Neo4J:

```python
#main bucket for parquet files to read into dataframe
final_df = spark.read.parquet("s3://<s3_bucket_name>/parquet_files/")

final_df.createOrReplaceTempView("final")

#count of dataframe
print(f'Total Rows: {final_df.count():,}')

#example of various forms of filtering
filtered_df = final_df.filter("<column_name> is not null")

#count of filtered dataframe
print(f'Total Rows after filter: {filtered_df.count():,}')
```

The next cell of code contains a pattern of building on top of the highly-configurable aspect of the Neo4J Spark Connector. In the first section, we have a simple Python dictionary that represents the base configuration for the Neo4J Datasource connection. 

After that, another dictionary (referenced as 'model_map' holds the configuration of each source/target/relationship we want to persist the data incoming from the dataframe to write to the database. 

We have a Python method called 'get_cypher_query' which stores all of the cypher used in writing the nodes. This is used as a wrapper function to used in the dataframe .query option in the 'write_neo4j_nodes' method. This allows us to make a simple call by passing in the dataframe and the node key in the get_cypher_query dictionary - Ex. write_neo4j_nodes(final_df, "node1").

Similarly, the write_neo4j_node_relationship is called with parameters for the dataframe variable and the key for to the model_map, making a one-line call leaving the configuration of the mapping of nodes/properties and relationships in one place to maintain. Ex. write_neo4j_node_relationship(no_empty_strings_in_column_df, "node1_node2")

In summary, the usage pattern in this code is:
    
    - Read data from parquet files into dataframes, transforming as needed.
    - Write all of the the nodes first
    - Then, write the relationships last, taking into considerations for write locks based off of the data model. Writing in this fashion takes advantage of Spark parallelism. Note: in the first cell was configured with 15 executor cores. If any locking was experienced during writes to Neo4j, I would have considered taking the number down, as well with the following option to take it down to 1 thread with <dataframe var>.coalesce(1) (code is in the notebook)

```python
"""
base configuration for neo4j
"""
config = {
    "bolt_url": "bolt://<neo_db_ip>>:7687",
    "database": "<database_name>",
    "user": "<user_name>",
    "password": "<password>",
    "batch_size": "500"
}

"""
map to descibe nodes and relationships passed into the neo read/write methods. Would like to move to yaml and maintain externally in git &/or s3 with validation.
"""
model_map = {
    "node1_node2" : {
        "source" : {
            "node": ":Node1",
            "property": "id_field:id"
        },
        "target" : {
            "node": ":Node2",
            "property": "id_field:id"
        },
        "relationship": {
            "rel": "IS_RELATED",
            "properties": ""
        }
    }
}

def get_cypher_query(node_name):
"""
method to accept node name as param and return a string of cypher, if needed.
"""
    cypher_map = {
        "node1": "MERGE (n:Node1 {id: event.id_field, createdDate: event.date})",
        "node2": "MERGE (n:Node1 {id: event.id_field, createdDate: event.date})"
    }
    return cypher_map[node_name.lower()]


def write_neo4j_nodes(df, node_name):
"""
write to neo4j with cypher, calling the 'get_cypher_query' with the name of the node as a param
"""
    df.write \
        .format("org.neo4j.spark.DataSource") \
        .mode("Overwrite") \
        .option("url", config["bolt_url"]) \
        .option("batch.size", config["batch_size"]) \
        .option("database", config["database"]) \
        .option("authentication.type", "basic") \
        .option("authentication.basic.username",  config["user"]) \
        .option("authentication.basic.password", config["password"]) \
        .option("query", get_cypher_query(node_name)) \
        .save()


def write_neo4j_node_relationship(df, model_map_key):
"""
method to write a source and target node with a relationship using the keys stragegy - https://neo4j.com/docs/spark/current/writing/#write-rel
The dataframe and map_model_key are passed in as parameters, the key resolves to the nodes/relationship held in the model_map_above
"""
    df.write \
        .format("org.neo4j.spark.DataSource") \
        .mode("overwrite") \
        .option("url", config["bolt_url"]) \
        .option("batch.size", config["batch_size"]) \
        .option("database", config["database"]) \
        .option("authentication.type", "basic") \
        .option("authentication.basic.username", config["user"]) \
        .option("authentication.basic.password", config["password"]) \
        .option("relationship", model_map[model_map_key]["relationship"]["rel"]) \
        .option("relationship.properties", model_map[model_map_key]["relationship"]["properties"]) \
        .option("relationship.save.strategy", "keys") \
        .option("relationship.source.labels", model_map[model_map_key]["source"]["node"]) \
        .option("relationship.source.save.mode", "overwrite") \
        .option("relationship.source.node.keys", model_map[model_map_key]["source"]["property"]) \
        .option("relationship.target.labels",  model_map[model_map_key]["target"]["node"]) \
        .option("relationship.target.node.keys", model_map[model_map_key]["target"]["property"]) \
        .option("relationship.target.save.mode", "overwrite") \
    .save()

    
def read_neo4j_nodes(node_name):
    spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", config["bolt_url"]) \
        .option("database", config["database"]) \
        .option("authentication.type", "basic") \
        .option("authentication.basic.username", config["user"]) \
        .option("authentication.basic.password", config["password"]) \
        .option("labels", node_name) \
        .load() \
        .show()
        
#write out nodes
write_neo4j_nodes(final_df, "node1")
write_neo4j_nodes(final_df, "node2")

#write out nodes/relationships
write_neo4j_node_relationship(no_empty_strings_in_column_df, "node1_node2")
```


