from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FakeFriends")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


lines = sc.textFile(
    "file:///Users/kacper.wozniak/Documents/udemy/spark-sandbox/datasets/fakefriends.csv"
)

rdd = lines.map(parse_line)
totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_by_age = totals_by_age.mapValues(lambda x: int(x[0] / x[1]))
average_by_age = average_by_age.sortByKey()

for result in average_by_age.collect():
    print(result)
