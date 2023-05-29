from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

with SparkSession.builder.appName("TempMinMax").getOrCreate() as spark:
    
    schema = StructType([
        StructField("station_id", StringType(), True),
        StructField("date", IntegerType(), True),
        StructField("measure_type", StringType(), True),
        StructField("temperature", FloatType(), True)])
    
    df = spark.read.schema(schema).csv("datasets/1800.csv")
    df.printSchema()
    
    min_temps = df.filter(df.measure_type == "TMIN")
    
    station_temps = min_temps.select("station_id", "temperature")
    
    min_temp_by_station = station_temps.groupBy("station_id")\
        .min("temperature")\
        .withColumn("min_temperature", func.col("min(temperature)"))\
        .select("station_id", "min_temperature")
    min_temp_by_station.show()
    
    min_temp_by_station_F = min_temp_by_station.withColumn(
        "min_temperature", func.round(func.col("min_temperature") * 0.1 * (9.0 / 5.0) + 32.0, 2).cast("float"))\
        .select("station_id", "min_temperature")\
        .sort("min_temperature")
    
    results = min_temp_by_station_F.collect()
    
    for result in results:
        print(result[0] + "\t{:.2f}F".format(result[1]))
