from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext,SparkConf

from pyspark.sql import SparkSession
from heapq import heappush, heappop  



if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: bigram <file>", file=sys.stderr)
    #     exit(-1)
    conf = SparkConf().set("spark.executor.memory", "10g")\
             .set("driver-memory","10g")\
             .set("spark.driver.maxResultSize","10g")
    print(conf.toDebugString())

    sc = SparkContext(conf=conf)


    lines = sc.textFile(sys.argv[1])

    
    sentences = lines.glom() \
                  .map(lambda x: " ".join(x)) \
                  .flatMap(lambda x: x.split(".")) \

    # sentences.collect().foreach(println)
    #print (sentences.collect())

    words = sentences.map(lambda word: (word.encode('utf-8')))\
                .map(lambda x : x.lower().split()) \
                .flatMap(lambda x: ((x[i], x[i + 1], x[i + 2]) for i in range(len(x) - 2))) \
                .map(lambda x : (x, 1)).reduceByKey(lambda x, y: x + y).filter(lambda x: x[1]>0)
    
    # last = words.map(lambda x: ((x[0][0],x[0][1]),(x[0][2],x[1]))).reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1]) > 2).map(f).collect()
    # word_count = sc.parallelize(last) 
    

    # word_count = sc.parallelize(last) 
    # word_count.repartition(1).saveAsTextFile("last1G.out")
    words.cache()
    
    first = words.map(lambda x: ((x[0][1],x[0][2]),(x[0][0],x[1])))
    middle = words.map(lambda x: ((x[0][0],x[0][2]),(x[0][1],x[1])))
    last = words.map(lambda x: ((x[0][0],x[0][1]),(x[0][2],x[1])))

    FirstOut = first.repartition(1).saveAsTextFile("first.out")
    middleOut = middle.repartition(1).saveAsTextFile("middle.out")
    LastOut = last.repartition(1).saveAsTextFile("last.out")

    # word_count = sc.parallelize(middle) 
    # word_count.repartition(1).saveAsTextFile("middle4G.out")

    sc.stop()
    

