from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

with SparkSession.builder.appName("Expenditure").getOrCreate() as spark:
    
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("item_id", FloatType(), True),
        StructField("spent", FloatType(), True)
    ])
    
    df = spark.read.schema(schema).csv("datasets/customer-orders.csv")
    df.printSchema()
    
    spent_by_customer = df.groupBy("customer_id")\
        .sum("spent")\
        .withColumn("spent_total", func.round(func.col("sum(spent)").cast("float"), 2))\
        .select("customer_id", "spent_total")\
        .sort("spent_total")
    
    for result in spent_by_customer.collect():
        print(result[0], "\t{:.2f}".format(result[1]))
