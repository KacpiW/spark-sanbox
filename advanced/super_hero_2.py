from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([StructField("id", IntegerType(), True),
                     StructField("name", StringType(), True)])

with SparkSession.builder.appName("SuperHero").getOrCreate() as spark:
    
    names = spark.read.schema(schema).option("sep", " ").csv("././datasets/marvel_names.txt")
    
    lines = spark.read.text("././datasets/marvel_graph.txt")
    
    connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])\
        .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1)\
        .groupBy("id").agg(func.sum("connections").alias("connections"))
        
    obscure_connections = connections.filter(func.col("connections") == '1')
    obscure_connections.show()
    
    min_value = connections.agg(func.min("connections")).collect()[0][0]
    obscure_connections = connections.filter(func.col("connections") == min_value)
    obscure_connections.join(names, "id").show()
    
    