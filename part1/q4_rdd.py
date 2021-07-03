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
      print("Usage: q4_rdd <file>", file=sys.stderr)
      exit(-1)
    
    #resultFile= open("resultFile_q4_rdd.txt","w+")    
    
    spark = SparkSession\
      .builder\
      .appName("q4_rdd")\
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
    
    def groupLustrum(x):
        if (x < 2005):
          return 1
        elif (x < 2010):
          return 2
        elif (x < 2015):
          return 3
        else:
          return 4
    
    inputMoviesRaw = sc.textFile(sys.argv[1]).map(lambda line: split_complex(line))
    inputMovies = inputMoviesRaw.map(lambda line : (line[0], line[2], line[3])) \
                  .map(lambda line: (line[0], (line[1], keep_year(line[2])))) #(movie_id, (summary, year))
    moviesAfter2000 = inputMovies.filter(lambda line : line[1][1] >= 2000) #keep after 2000
    
    inputMovieGenres = sc.textFile(sys.argv[2]).map(lambda line : (line.split(',')[0], line.split(',')[1])) #(movie_id, genre)
    dramaGenre = inputMovieGenres.filter(lambda line : line[1] == 'Drama') #keep drama
    
    dramaMovies = dramaGenre.join(moviesAfter2000).map(lambda line : (line[1][1][1], line[1][1][0])) #(movie_id, (genre, (summary, year))) -> (year, summary)
    
    summariesPer5Years = dramaMovies.map(lambda line : (groupLustrum(line[0]), len(line[1]))).aggregateByKey( StatCounter(), StatCounter.merge, StatCounter.mergeStats).mapValues(lambda s: (s.count(), s.sum())) #(pentaetia, length_summary) -> (pentaetia, (number_summaries, total_length_summaries))
    averageSummariesPer5Years = summariesPer5Years.map(lambda x : (x[0], (x[1][1]/x[1][0]))) #(pentaetia, average_length)

    #averageSummariesPer5Years.saveAsTextFile('hdfs:/atds_project_21/results/q4_rdd.txt')

    print("-----------------------------------------------------------------------------")
    for i in averageSummariesPer5Years.take(50):
      print(i)

    '''toPrint = moviesAfter2000.take(100000)
    for i in range (len(toPrint)):	
      resultFile.write('%s\n'%str(toPrint[i]))'''
    #resultFile.close()
    
    spark.stop()