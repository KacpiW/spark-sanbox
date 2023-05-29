from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import round, col


def mapper(line):
    fields = line.split(",")
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")),
               age=int(fields[2]), numFriends=int(fields[3]))


def order_mapper(line):
    fields = line.split(",")
    customer_id = int(fields[0])
    item_id = int(fields[1])
    value = float(fields[2])
    return Row(customer_id=customer_id, item_id=item_id, value=value)


with SparkSession.builder.appName("socialNetwork").getOrCreate() as spark:
    # SOCIAL NETWORK
    lines = spark.sparkContext.textFile("datasets/fakefriends.csv")
    people = lines.map(mapper)

    schemaPeople = spark.createDataFrame(people).cache()
    schemaPeople.createOrReplaceTempView("people")

    teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    for teen in teenagers.collect():
        print(teen)

    schemaPeople.groupBy("age").count().orderBy("age").show()

    # ORDERS
    order_lines = spark.sparkContext.textFile("datasets/customer-orders.csv")
    orders = order_lines.map(order_mapper)

    schema_order = spark.createDataFrame(orders).cache()
    schema_order.createOrReplaceTempView("orders")

    schema_order.groupBy("customer_id")\
        .avg("value")\
        .withColumnRenamed("avg(value)", "average_value")\
        .withColumn("average_value", col("average_value").cast("float"))\
        .withColumn("average_value", round("average_value", 4))\
        .orderBy("average_value")\
        .show()
