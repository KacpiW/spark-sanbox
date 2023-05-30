from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)


def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


lines = sc.textFile(
    "file:////Users/kacper.wozniak/Documents/udemy/spark-sanbox/datasets/Book"
)

words = lines.flatMap(normalize_words)
words = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
words = words.map(lambda x: (x[1], x[0])).sortByKey()
words_count = words.collect()

for word in words_count:
    clean_word = word[1].encode('ascii', 'ignore').decode('utf-8')
    if clean_word:
        print(clean_word, word[0])
