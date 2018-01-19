from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext,SparkConf

if __name__ == "__main__":

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1]).repartition(150)

    
    sentences = lines.glom() \
                  .map(lambda x: " ".join(x)) \
                  .flatMap(lambda x: x.split(".")) \
    words = sentences.map(lambda word: (word.encode('utf-8')))\
                .map(lambda x : x.lower().split()) \
                .flatMap(lambda x: ((x[i], x[i + 1], x[i + 2]) for i in range(len(x) - 2))) \
                .map(lambda x : (x, 1)).reduceByKey(lambda x, y: x + y)

    words10 = words.filter(lambda x: x[1]>10).cache()
    
    first = words10.map(lambda x: ((x[0][1],x[0][2]),(x[0][0],x[1])))
    FirstOut = first.repartition(1).saveAsTextFile("whole10G_Greater10_1.out")

    middle = words10.map(lambda x: ((x[0][0],x[0][2]),(x[0][1],x[1])))
    middleOut = middle.repartition(1).saveAsTextFile("whole10G_Greater10_2.out")

    last = words10.map(lambda x: ((x[0][0],x[0][1]),(x[0][2],x[1])))
    LastOut = last.repartition(1).saveAsTextFile("whole10G_Greater10_3.out")

    words20 = words10.filter(lambda x: x[1]>10).cache()
    
    first = words20.map(lambda x: ((x[0][1],x[0][2]),(x[0][0],x[1])))
    FirstOut = first.repartition(1).saveAsTextFile("whole10G_Greater20_1.out")

    middle = words20.map(lambda x: ((x[0][0],x[0][2]),(x[0][1],x[1])))
    middleOut = middle.repartition(1).saveAsTextFile("whole10G_Greater20_2.out")

    last = words20.map(lambda x: ((x[0][0],x[0][1]),(x[0][2],x[1])))
    LastOut = last.repartition(1).saveAsTextFile("whole10G_Greater20_3.out")

    words30 = words20.filter(lambda x: x[1]>30).cache()
    
    first = words30.map(lambda x: ((x[0][1],x[0][2]),(x[0][0],x[1])))
    FirstOut = first.repartition(1).saveAsTextFile("whole10G_Greater30_1.out")

    middle = words30.map(lambda x: ((x[0][0],x[0][2]),(x[0][1],x[1])))
    middleOut = middle.repartition(1).saveAsTextFile("whole10G_Greater30_2.out")

    last = words30.map(lambda x: ((x[0][0],x[0][1]),(x[0][2],x[1])))
    LastOut = last.repartition(1).saveAsTextFile("whole10G_Greater30_3.out")
    sc.stop()
    

