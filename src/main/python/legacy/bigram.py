from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: bigram <file>", file=sys.stderr)
    #     exit(-1)


    sc = SparkContext()
    lines = sc.textFile("/Users/xuanyu/Desktop/wiki_test.txt", 1)

    sentences = lines.glom() \
                  .map(lambda x: " ".join(x)) \
                  .flatMap(lambda x: x.split(".")) \

    words = sentences.map(lambda word: (word.encode('utf-8')))\
                .map(lambda x : x.lower().split()) \
                .flatMap(lambda x: ((x[i], x[i + 1], x[i+2]) for i in range(len(x) - 2))) \
                .map(lambda x : (x, 1)).reduceByKey(lambda x, y: x + y)\
                .sortBy(lambda x: x[1], False).take(100)

    word_count = sc.parallelize(words)
    word_count.repartition(1).saveAsTextFile("first.out")

    sc.stop()