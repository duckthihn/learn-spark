from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]") #setMaster() lay bao nhieu core #

sc = SparkContext(conf = conf)

word_rdd = sc.parallelize(["CAM dieu Thuoc lao nhu Trieu tu long cam dao"]) \
    .flatMap(lambda word: word.split(" ")) \
    .map(lambda phamthoai : phamthoai.lower()) \

print(word_rdd.collect())


stop_words_rdd = sc.parallelize(["dieu thuoc dao cam"]) \
    .flatMap(lambda word: word.split(" "))

final_rdd = word_rdd.subtract(stop_words_rdd)

print(final_rdd.collect())
