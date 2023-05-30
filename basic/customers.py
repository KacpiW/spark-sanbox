from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customers")
sc = SparkContext(conf=conf)

orders = sc.textFile("datasets/customer-orders.csv")


def parse_orders(line):
    fields = line.split(",")
    customer_id = fields[0]
    item_id  = fields[1]
    order_value = float(fields[2])
    return (customer_id, item_id , order_value)


orders = orders.map(parse_orders)
orders = orders.map(lambda x: (x[0], round(x[2], 2))).reduceByKey(lambda x, y: x + y)
orders = orders.map(lambda x: (x[1], x[0])).sortByKey()
orders = orders.collect()

for order in orders:
    print(order[1], order[0])
