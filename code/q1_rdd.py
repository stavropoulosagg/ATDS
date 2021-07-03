from __future__ import print_function
from __future__ import division

import sys
from operator import add

from pyspark.sql import SparkSession
from io import StringIO
import csv
from datetime import datetime
from pyspark.statcounter import StatCounter

if __name__ == "__main__":
    
    if len(sys.argv) != 2:
      print("Usage: q1_rdd <file>", file=sys.stderr)
      exit(-1)
    
    #resultFile= open("resultFile_q1_rdd.txt","w+")    
    
    spark = SparkSession\
      .builder\
      .appName("q1_rdd")\
      .getOrCreate()

    sc = spark.sparkContext   
    
    def split_complex(x): 
        return list(csv.reader(StringIO(x), delimiter=','))[0]
    
    def keep_year(x):
        year = x.split("-")[0]
        if year.isnumeric():
          return int(year)
        else:
          return 0
    
    def computeProfit(x, y):
        revenue = int(y)
        cost = int(x)
        if (cost == 0):
          return 0
        else:
          profit = (revenue - cost) / cost * 100
          return profit
    
    def maxProfit (x, y):
      if x[1] > y[1]:
        return x
      else:
        return y
    
    inputMoviesRaw = sc.textFile(sys.argv[1]).map(lambda line: split_complex(line))  
    inputMovies = inputMoviesRaw.map(lambda line : (line[1], line[3], line[5], line[6])) \
                  .map(lambda line: (line[0], keep_year(line[1]), int(line[2]), int(line[3]))) #(title, year, cost, revenue)
    
    filteredMovies = inputMovies.filter(lambda line : line[1] >= 2000).filter(lambda line : line[2] > 0).filter(lambda line : line[3] > 0)
    moviesProfit = filteredMovies.map(lambda line : (line[0], line[1], computeProfit(line[2], line[3])))
    
    moviesPerYear = moviesProfit.map(lambda line : (line[1], (line[0], line[2]))) \
                    .aggregateByKey(("",0), maxProfit, maxProfit).sortByKey()


    #moviesPerYear.saveAsTextFile('hdfs:/atds_project_21/results/q1_rdd.txt')

    print("-----------------------------------------------------------------------------")
    for i in moviesPerYear.take(50):
      print(i)

    '''toPrint = moviesAfter2000.take(100000)
    for i in range (len(toPrint)):	
      resultFile.write('%s\n'%str(toPrint[i]))'''
    #resultFile.close()

    spark.stop()