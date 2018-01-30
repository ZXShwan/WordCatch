from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
#from Queue import PriorityQueue

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
    return newTemp


if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: bigram <file>", file=sys.stderr)
    #     exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)

    sentences = lines.glom() \
                  .map(lambda x: " ".join(x)) \
                  .flatMap(lambda x: x.split(".")) 

    words = sentences.map(lambda word: (word.encode('utf-8')))\
                .map(lambda x : x.lower().split()) \
                .flatMap(lambda x: ((x[i], x[i + 1], x[i+2], x[i+3]) for i in range(len(x) - 3))) \
                .map(lambda x : (x, 1)).reduceByKey(lambda x, y: x + y)
    

    last = words.map(lambda x: ((x[0][1],x[0][2],x[0][3]),(x[0][0],x[1]))).reduceByKey(lambda x, y: x+y).filter(lambda x: len(x[1])>6).map(f).collect()
    
    words_count=sc.parallelize(last)
    #words_count.foreach(f)
    words_count.repartition(16).saveAsTextFile("FourFristOut")
    
    # spark = SparkSession \
    #  .builder \
    #  .appName("Python Spark SQL basic example") \
    #  .config("spark.some.config.option", "some-value") \
    #  .getOrCreate()

    # dataf = spark.createDataFrame(words_count.repartition(1))
    # dataf.show(truncate=False)
    #dataf.write.format("json").save("/Users/xuanyu/Desktop/result.json")

    # #word_count.repartition(1).toDF().write.format("json").save("/Users/xuanyu/Desktop/result.json")

                         #words_count.repartition(1).saveAsTextFile("/Users/xuanyu/Desktop/first.out")
    sc.stop()
   

