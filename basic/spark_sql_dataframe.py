from pyspark.sql import SparkSession
from pyspark.sql.functions import cast, col


with SparkSession.builder.appName("SparkSQL").getOrCreate() as spark:
    
    people = spark.read.option("header", "true").option("inferSchema", "true")\
        .csv("datasets/fakefriends-header.csv")
        
    people.printSchema()
    
    people.select("name").show()
    
    people.filter(people.age < 21).show()
    
    people.groupBy("age").count()\
        .withColumn("age", col("age").cast("int"))\
        .orderBy("age")\
        .show()
    
    people.select(people.name, people.age + 10).show()