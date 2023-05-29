from pyspark.sql import SparkSession, Row

def mapper(line):
    fields = line.split(",")
    return Row(id=int(fields[0]), 
               name=str(fields[1]).encode("utf-8"),
               age=int(fields[2]),
               friends=int(fields[3]))
    
    
with SparkSession.builder.appName('Friends').getOrCreate() as spark:
    
    lines = spark.sparkContext.textFile("datasets/fakefriends.csv")
    friends = lines.map(mapper)
    
    friends = spark.createDataFrame(friends).cache()
    friends.createOrReplaceTempView("friends")
    
    friends.groupBy("age").avg("friends").show()
    