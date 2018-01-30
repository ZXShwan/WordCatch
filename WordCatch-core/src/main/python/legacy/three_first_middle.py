from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext

from pyspark.sql import SparkSession
from heapq import heappush, heappop  

class PriorityQueue:  
    def __init__(self):  
        self._queue = []
        self._index = 0 
  
    def put(self, item, priority):  
        heappush(self._queue, (-priority, item))  
        self._index += 1
  
    def get(self): 
        self._index -= 1 
        return heappop(self._queue)[-1]

    def empty(self):
        return self._index == 0


def f(word):
    words = word[1]
    q = PriorityQueue()
    i = 0
    while i<len(words):
        q.put((words[i],words[i+1]),words[i+1])
        i=i+2

    need = 10
    temp =[]
    while not q.empty() and need > 0:
        temp += [q.get()]
        need = need-1

    newTemp = [word[0],temp]
    print(newTemp)
    return newTemp

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

    words = sentences.map(lambda word: (word.encode('utf-8')))\
                .map(lambda x : x.lower().split()) \
                .flatMap(lambda x: ((x[i], x[i + 1], x[i + 2]) for i in range(len(x) - 2))) \
                .map(lambda x : (x, 1)).reduceByKey(lambda x, y: x + y).filter(lambda x: x[1]>20)
    
    # last = words.map(lambda x: ((x[0][0],x[0][1]),(x[0][2],x[1]))).reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1]) > 2).map(f).collect()
    # word_count = sc.parallelize(last) 
    

    # word_count = sc.parallelize(last) 
    # word_count.repartition(1).saveAsTextFile("last1G.out")

    middle = words.map(lambda x: ((x[0][0],x[0][2]),(x[0][1],x[1]))).collect()

    word_count = sc.parallelize(middle) 
    word_count.saveAsTextFile("middle1G.out")

    # first = words.map(lambda x: ((x[0][1],x[0][2]),(x[0][0],x[1]))).reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1]) > 2).map(f).collect()
    # word_count = sc.parallelize(first) 
    # word_count.repartition(1).saveAsTextFile("first1G.out")
    # (('easy', 'to'), (('easy', 'to', 'use'), 1))

    #word_count.repartition(1).saveAsTextFile("/Users/xuanyu/Desktop/count.out")
    
    # spark = SparkSession \
    # .builder \
    # .appName("Python Spark SQL basic example") \
    # .config("spark.some.config.option", "some-value") \
    # .getOrCreate()

    # dataf = spark.createDataFrame(word_count.repartition(1)) 
    # dataf.write.format("json").save("/Users/xuanyu/Desktop/result.json")

    word_count.repartition(1).toDF().write.format("json").save("test")

    #word_count.saveAsTextFile(sys.argv[2])
    sc.stop()
    

