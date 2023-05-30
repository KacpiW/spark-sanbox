from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import pyspark.sql.functions as func
import codecs


schema = StructType([StructField("user_id", IntegerType(), True),
                     StructField("movie_id", IntegerType(), True),
                     StructField("rating", IntegerType(), True),
                     StructField("timestamp", LongType(), True)])


def load_movie_names() -> Dict[int, str]:
    movie_names = {}
    with codecs.open("././datasets/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as file:
        for line in file:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


def lookup_name(movie_id: int) -> str:
    return movie_names.value[movie_id]


with SparkSession.builder.appName("PopularMovies").getOrCreate() as spark:

    movie_names = spark.sparkContext.broadcast(load_movie_names())

    user_ratings = spark.read.option("sep", "\t")\
        .schema(schema)\
        .csv("././datasets/ml-100k/u.data")

    movie_ratings = user_ratings.groupBy("movie_id")\
        .count()

    lookup_name_udf = func.udf(lookup_name)

    movie_with_name = movie_ratings\
        .withColumn("title", lookup_name_udf(func.col("movie_id")))\
        .orderBy(func.desc("count"))
    movie_with_name.show()
