from pyspark.sql import SparkSession
from pyspark.sql import functions as func

with SparkSession.builder.appName("WordCount").getOrCreate() as spark:

    input_df = spark.read.text("datasets/Book")

    words = input_df.select(
        func.explode(
            func.split(input_df.value, '\\W+')).alias("word")
        )

    words.filter(words.word != "")

    lowercaseWords = words.select(func.lower(words.word).alias("word"))

    wordCounts = lowercaseWords.groupBy("word").count()

    wordCountsSorted = wordCounts.sort("count")

    wordCountsSorted.show(wordCountsSorted.count())
