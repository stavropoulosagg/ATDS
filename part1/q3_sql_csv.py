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
      print("Usage: q3_sql_csv <file>", file=sys.stderr)
      exit(-1)
    
    #resultFile= open("resultFile_q2_sql_csv.txt","w+")    
    
    spark = SparkSession\
      .builder\
      .appName("q3_sql_csv")\
      .getOrCreate()

    #sc = spark.sparkContext   

    
    inputGenres = spark.read.format('csv').options(header='false', inferSchema='true').load(sys.argv[1])
    inputGenres.registerTempTable("genres")
    #spark.sql("select _c3, _c5, _c6 from movies order by _c3 desc").show(30)
    inputRatings = spark.read.format('csv').options(header='false', inferSchema='true').load(sys.argv[2])
    inputRatings.registerTempTable("ratings")
    
    total_ratings_per_movie = spark.sql("select _c1 as movie_id, count(*) as ratings_movie from ratings group by movie_id")
    total_ratings_per_movie.registerTempTable("total_ratings_per_movie") #[movie_id, total_ratings]
    
    sum_ratings_per_movie = spark.sql("select _c1 as movie_id, sum(_c2) as sum_ratings_movie from ratings group by movie_id")
    #sum_ratings_per_user.show()
    sum_ratings_per_movie.registerTempTable("sum_ratings_per_movie") #[movie_id, sum_ratings]
    
    average_ratings_per_movie = spark.sql("with staging as (select a.movie_id, a.sum_ratings_movie, b.ratings_movie from sum_ratings_per_movie as a inner join total_ratings_per_movie as b on (a.movie_id = b.movie_id)) select movie_id, (sum_ratings_movie/ratings_movie) as average_ratings_movie from staging")
    #average_ratings_per_movie.show()
    average_ratings_per_movie.registerTempTable("average_ratings_per_movie") #[movie_id, average_rating]
    
    genres_ratings = spark.sql("select a._c1 as genre, b.average_ratings_movie from genres as a inner join average_ratings_per_movie as b on a._c0 = b.movie_id")
    #genres_ratings.show(50)
    genres_ratings.registerTempTable("genres_ratings") #[genre, average_rating_of_1_movie]
    
    total_ratings_per_genre = spark.sql("select genre, count(*) as ratings_genre from genres_ratings group by genre")
    total_ratings_per_genre.registerTempTable("total_ratings_per_genre") #[genre, total_movies_of_genre]
    
    sum_ratings_per_genre = spark.sql("select genre, sum(average_ratings_movie) as sum_ratings_genre from genres_ratings group by genre")
    sum_ratings_per_genre.registerTempTable("sum_ratings_per_genre") #[genre, sum_of_movie_of_genre]
    
    average_ratings_per_genre = spark.sql("with staging as (select a.genre, a.sum_ratings_genre, b.ratings_genre from sum_ratings_per_genre as a inner join total_ratings_per_genre as b on (a.genre = b.genre)) select genre, (sum_ratings_genre/ratings_genre) as average_ratings_genre, ratings_genre from staging")
    average_ratings_per_genre.show(50) #[genre, average_rating, total_ratings_of_genre]
    
    
    #resultFile.close()

    spark.stop()