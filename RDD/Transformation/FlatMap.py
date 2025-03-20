from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]") #setMaster() lay bao nhieu core #

sc = SparkContext(conf = conf)

file_rdd = sc.textFile("/home/duckthihn/PycharmProjects/DE-ETL/RDD/data/data.txt")

words_rdd = file_rdd.flatMap(lambda word: word.split(" "))

# for word in words_rdd.collect():
#     print(word)

print(words_rdd.collect())