from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: bigram <file>", file=sys.stderr)
    #     exit(-1)


    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)

    sentences = lines.glom() \
                  .map(lambda x: " ".join(x)) \
                  .flatMap(lambda x: x.split(".")) \

    words = sentences.map(lambda word: (word.encode('utf-8')))\
                .map(lambda x : x.lower().split()) \
                .flatMap(lambda x: ((x[i], x[i + 1], x[x + 2]) for i in range(len(x) - 2))) \
                .map(lambda x : (x, 1)).reduceByKey(lambda x, y: x + y)

    last = words.map(lambda x: ((x[0][0],x[0][1]),x)).reduceByKey(lambda x,y: x+y)
    word_count = sc.parallelize(last)
    #word_count.repartition(1).saveAsTextFile("/Users/xuanyu/Desktop/count.out")
    
    # spark = SparkSession \
    # .builder \
    # .appName("Python Spark SQL basic example") \
    # .config("spark.some.config.option", "some-value") \
    # .getOrCreate()

    # dataf = spark.createDataFrame(word_count.repartition(1)) 
    # dataf.write.format("json").save("/Users/xuanyu/Desktop/result.json")

    # #word_count.repartition(1).toDF().write.format("json").save("/Users/xuanyu/Desktop/result.json")

    word_count = sc.parallelize(words)
    word_count.repartition(1).saveAsTextFile("xuan.out")
    
    

    sc.stop()
    

