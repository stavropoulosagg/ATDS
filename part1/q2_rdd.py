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
      print("Usage: q2_rdd <file>", file=sys.stderr)
      exit(-1)
    
    #resultFile= open("resultFile_q2_rdd.txt","w+")    
    
    spark = SparkSession\
      .builder\
      .appName("q2_rdd")\
      .getOrCreate()

    sc = spark.sparkContext   
    
    inputRatings = sc.textFile(sys.argv[1]).map(lambda line: (int(line.split(',')[0]), float(line.split(',')[2])))
    
    totalUsers = inputRatings.reduceByKey(lambda x, y : x + y).count() #total number of users - count unique lines
    
    ratingsPerUser = inputRatings.aggregateByKey( StatCounter(), StatCounter.merge, StatCounter.mergeStats).mapValues(lambda s: (s.count(), s.sum())) #(user, (number_of_ratings, sum_of_ratings))
    
    averageRatingsPerUser = ratingsPerUser.map(lambda x : (x[0], (x[1][1]/x[1][0]))).sortByKey() #(user, average_rating)
    
    usersAbove3 = averageRatingsPerUser.filter(lambda line : line[1] > float(3))
    
    totalUsersAbove3 = usersAbove3.count()


    print("-----------------------------------------------------------------------------")
    print(totalUsers)
    print(totalUsersAbove3)
    print(totalUsersAbove3/totalUsers*100) 
    '''for i in averageRatingsPerUser.take(50):
      print(i)'''

    '''toPrint = moviesAfter2000.take(100000)
    for i in range (len(toPrint)):	
      resultFile.write('%s\n'%str(toPrint[i]))'''
    #resultFile.close()

    spark.stop()