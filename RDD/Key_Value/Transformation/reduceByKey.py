from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]").set("spark.executor.memory", "4g")
#setMaster() lay bao nhieu core

sc = SparkContext(conf = conf)

rdd = sc.parallelize([("kakashi-debt", 10.5),
                      ("naruto-debt", 90.5),
                      ("naruto-debt", 10.5),
                      ("kakashi-debt", 30.5),
                      ("conan-debt", 100.5),
                      ])

bill_debt = rdd.reduceByKey(lambda key, value: key + value) # key + key , value + value
print(bill_debt.collect())