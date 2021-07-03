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
    
    if len(sys.argv) != 3:
      print("Usage: q3_rdd <file>", file=sys.stderr)
      exit(-1)
    
    #resultFile= open("resultFile_q3_rdd.txt","w+")    
    
    spark = SparkSession\
      .builder\
      .appName("q3_rdd")\
      .getOrCreate()

    sc = spark.sparkContext   
    
    def split_complex(x): 
        return list(csv.reader(StringIO(x), delimiter=','))[0]
    
    def maxProfitComb(x, y):
      if x[1] > y[1]:
        return x
      else:
        return y
    
    inputMovieGenres = sc.textFile(sys.argv[1]).map(lambda line : (line.split(',')[0], line.split(',')[1])) #(movie_id, genre)
    
    inputRatings = sc.textFile(sys.argv[2]).map(lambda line : (line.split(',')[1], float(line.split(',')[2]))) #(movie_id, rating)
    
    ratingsPerMovie = inputRatings.aggregateByKey( StatCounter(), StatCounter.merge, StatCounter.mergeStats).mapValues(lambda s: (s.count(), s.sum())) #(movie_id, (count_ratings, sum_ratings))
    averageRatingsPerMovie = ratingsPerMovie.map(lambda x : (x[0], (x[1][1]/x[1][0]))) #(movie_id, average_rating)
    
    ratingsPerGenre = inputMovieGenres.join(averageRatingsPerMovie).map(lambda line : (line[1][0], line[1][1])).aggregateByKey( StatCounter(), StatCounter.merge, StatCounter.mergeStats).mapValues(lambda s: (s.count(), s.sum())) #(movie_id, (genre, average_rating)) -> (genre, average_rating_of_1_movie) -> (genre, (count_ratings, sum_ratings))
    averageRatingsPerGenre = ratingsPerGenre.map(lambda x : (x[0], (x[1][1]/x[1][0]), x[1][0])) #(genre, average_rating, total_ratings)

    #averageRatingsPerGenre.saveAsTextFile('hdfs:/atds_project_21/results/q3_rdd.txt')

    print("-----------------------------------------------------------------------------")
    for i in averageRatingsPerGenre.take(50):
      print(i)

    '''toPrint = moviesAfter2000.take(100000)
    for i in range (len(toPrint)):	
      resultFile.write('%s\n'%str(toPrint[i]))'''
    #resultFile.close()

    spark.stop()