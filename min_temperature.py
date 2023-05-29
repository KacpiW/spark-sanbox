from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperature")
sc = SparkContext(conf=conf)

lines = sc.textFile(
    "file:///Users/kacper.wozniak/Documents/udemy/spark/datasets/1800.csv")


def parse_lines(line):
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temperature = int(fields[3])
    return (station_id, entry_type, temperature)


lines = lines.map(parse_lines)
min_temps = lines.filter(lambda x: "TMIN" in x[1])

station_temps = min_temps.map(lambda x: (x[0], x[2]))
min_station_temps = station_temps.reduceByKey(lambda x, y: min(x, y))

for result in min_station_temps.collect():
    print(result)

station_avg_temps = lines.map(lambda x: (x[0], x[2]))
station_avg_temps = station_avg_temps.mapValues(lambda x: (x, 1)).reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1]))
station_avg_temps = station_avg_temps.mapValues(lambda x: int(x[0] / x[1]))

for result in station_avg_temps.collect():
    print(result)
