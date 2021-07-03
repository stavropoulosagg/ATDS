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
      print("Usage: q5_sql_csv <file>", file=sys.stderr)
      exit(-1)
    
    #resultFile= open("resultFile_q5_sql_csv.txt","w+")    
    
    spark = SparkSession\
      .builder\
      .appName("q5_sql_csv")\
      .getOrCreate()

    #sc = spark.sparkContext   
      
    
    
    #spark.udf.register("groupLustrum", groupLustrum)

    
    inputMovies = spark.read.format('csv').options(header='false', inferSchema='true').load(sys.argv[1])
    inputMovies.registerTempTable("movies")
    #spark.sql("select _c3, _c5, _c6 from movies order by _c3 desc").show(30)
    inputGenres = spark.read.format('csv').options(header='false', inferSchema='true').load(sys.argv[2])
    inputGenres.registerTempTable("genres")
    inputRatings = spark.read.format('csv').options(header='false', inferSchema='true').load(sys.argv[3])
    inputRatings.registerTempTable("ratings")
    
    ratings_with_genres_popularities = spark.sql("with movies_genres as (select a._c0 as movie_id, a._c1 as title, b._c1 as genre, a._c7 as popularity from movies as a inner join genres as b on a._c0 = b._c0) select c.genre, d._c0 as user, c.title, d._c2 as rating, c.popularity from movies_genres as c inner join ratings as d on c.movie_id = d._c1")
    ratings_with_genres_popularities.registerTempTable("ratings_with_genres_popularities")
    
    max_user_rating_per_genre_no_title = spark.sql("with staging as (select genre, user, max(rating) as max_rating from ratings_with_genres_popularities group by genre, user) select a.genre, a.user, a.rating, max(a.popularity) as max_popularity from ratings_with_genres_popularities as a inner join staging as b on a.genre = b.genre and a.user = b.user and a.rating = b.max_rating group by a.genre, a.user, a.rating")
    max_user_rating_per_genre_no_title.registerTempTable("max_user_rating_per_genre_no_title")
    max_user_rating_per_genre = spark.sql("select a.genre, a.user, a.title, a.rating, a.popularity from ratings_with_genres_popularities as a inner join max_user_rating_per_genre_no_title as b on a.genre = b.genre and a.user = b.user and a.rating = b.rating and a.popularity = b.max_popularity")
    max_user_rating_per_genre.registerTempTable("max_user_rating_per_genre")
    
    min_user_rating_per_genre_no_title = spark.sql("with staging as (select genre, user, min(rating) as min_rating from ratings_with_genres_popularities group by genre, user) select a.genre, a.user, a.rating, max(a.popularity) as max_popularity from ratings_with_genres_popularities as a inner join staging as b on a.genre = b.genre and a.user = b.user and a.rating = b.min_rating group by a.genre, a.user, a.rating")
    min_user_rating_per_genre_no_title.registerTempTable("min_user_rating_per_genre_no_title")
    min_user_rating_per_genre = spark.sql("select a.genre, a.user, a.title, a.rating, a.popularity from ratings_with_genres_popularities as a inner join min_user_rating_per_genre_no_title as b on a.genre = b.genre and a.user = b.user and a.rating = b.rating and a.popularity = b.max_popularity")
    min_user_rating_per_genre.registerTempTable("min_user_rating_per_genre")

    genres_users = spark.sql("select a._c1 as genre, b._c0 as user, b._c2 as rating from genres as a inner join ratings as b on a._c0 = b._c1")
    genres_users.registerTempTable("genres_users")
    
    user_ratings_per_genre = spark.sql("select genre, user, count(*) as user_ratings from genres_users group by genre, user")
    #user_ratings_per_genre.show()
    user_ratings_per_genre.registerTempTable("user_ratings_per_genre")
    
    max_user_per_genre = spark.sql("with max_ratings_per_genre as (select genre, max(user_ratings) as max_user_ratings from user_ratings_per_genre group by genre) select a.genre, b.user, a.max_user_ratings from max_ratings_per_genre as a inner join user_ratings_per_genre as b on a.genre = b.genre and a.max_user_ratings = b.user_ratings")
    max_user_per_genre.registerTempTable("max_user_per_genre")
    
    res = spark.sql("with staging as (select a.genre, a.user, a.max_user_ratings, b.title, b.rating from max_user_per_genre as a inner join max_user_rating_per_genre as b on a.genre = b.genre and a.user = b.user) select c.genre, c.user, c.max_user_ratings, c.title as title_max, c.rating as rating_max, d.title as title_min, d.rating as rating_min from staging as c inner join min_user_rating_per_genre as d on c.genre = d.genre and c.user = d.user order by c.genre")
    res.show(50)
    
    #resultFile.close()

    spark.stop()