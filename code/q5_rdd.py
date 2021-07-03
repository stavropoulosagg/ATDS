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
    
    if len(sys.argv) != 4:
      print("Usage: q5_rdd <file>", file=sys.stderr)
      exit(-1)
    
    #resultFile= open("resultFile_q5_rdd.txt","w+")    
    
    spark = SparkSession\
      .builder\
      .appName("q5_rdd")\
      .getOrCreate()

    sc = spark.sparkContext   
    
    def split_complex(x): 
        return list(csv.reader(StringIO(x), delimiter=','))[0]
    
    def findMaxRating (x, y):
      if ((x[1] > y[1]) or ((x[1] == y[1]) and (x[2] > y[2]))):
        return x
      else:
        return y
    
    def findMinRating (x, y):
      if ((x[1] < y[1]) or ((x[1] == y[1]) and (x[2] > y[2]))):
        return x
      else:
        return y
    
    def findMaxUser (x, y):
      if x[1] > y[1]:
        return x
      else:
        return y
    
    inputMoviesRaw = sc.textFile(sys.argv[1]).map(lambda line: split_complex(line))
    
    moviesPopularities = inputMoviesRaw.map(lambda line : (line[0], (line[1], float(line[7]))))
    
    inputRatings = sc.textFile(sys.argv[3]).map(lambda line : (line.split(',')[1], (line.split(',')[0], float(line.split(',')[2]))))
    
    inputMovieGenres = sc.textFile(sys.argv[2]).map(lambda line : (line.split(',')[0], line.split(',')[1]))
    
    ratingsWithGenresPopularities = inputRatings.join(moviesPopularities).join(inputMovieGenres).map(lambda line : ((line[1][1], line[1][0][0][0]), (line[1][0][1][0], line[1][0][0][1], line[1][0][1][1]))) #((genre, user), (title, rating, popularity))
    maxUserRatingPerGenre = ratingsWithGenresPopularities.aggregateByKey(("", 0, 0), findMaxRating, findMaxRating)
    minUserRatingPerGenre = ratingsWithGenresPopularities.aggregateByKey(("", 10, 0), findMinRating, findMinRating)
    
    genresUsers = inputMovieGenres.join(inputRatings).map(lambda line : ((line[1][0], line[1][1][0]), line[1][1][1])) #((genre, user), rating)
    userRatingsPerGenre = genresUsers.groupByKey().map(lambda line : (line[0], len(line[1]))) #((genre, user), ratings)
    maxUserPerGenre = userRatingsPerGenre.map(lambda line : (line[0][0], (line[0][1], line[1]))).aggregateByKey(("", 0), findMaxUser, findMaxUser) #(genre, (maxuser, noofratings))
    
    res = maxUserPerGenre.map(lambda line : ((line[0], line[1][0]), line[1][1])).join(maxUserRatingPerGenre).join(minUserRatingPerGenre).map(lambda line : (line[0][0], (line[0][1], line[1][0][0], line[1][0][1][0], line[1][0][1][1], line[1][1][0], line[1][1][1]))).sortByKey()

    #res.saveAsTextFile('hdfs:/atds_project_21/results/q5_rdd.txt')

    print("-----------------------------------------------------------------------------")
    for i in res.take(50):
      print(i)

    '''toPrint = moviesAfter2000.take(100000)
    for i in range (len(toPrint)):	
      resultFile.write('%s\n'%str(toPrint[i]))'''
    #resultFile.close()

    spark.stop()