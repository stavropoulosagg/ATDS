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
      print("Usage: q4_sql_csv <file>", file=sys.stderr)
      exit(-1)
    
    #resultFile= open("resultFile_q4_sql_csv.txt","w+")    
    
    spark = SparkSession\
      .builder\
      .appName("q4_sql_csv")\
      .getOrCreate()

    #sc = spark.sparkContext   
      
    def groupLustrum(x):
        if (x < 2005):
          return 1
        elif (x < 2010):
          return 2
        elif (x < 2015):
          return 3
        else:
          return 4
    
    spark.udf.register("groupLustrum", groupLustrum)

    
    inputMovies = spark.read.format('csv').options(header='false', inferSchema='true').load(sys.argv[1])
    inputMovies.registerTempTable("movies")
    #spark.sql("select _c3, _c5, _c6 from movies order by _c3 desc").show(30)
    inputGenres = spark.read.format('csv').options(header='false', inferSchema='true').load(sys.argv[2])
    inputGenres.registerTempTable("genres")
    
    drama_genre = spark.sql("select * from genres where _c1 = 'Drama'")
    drama_genre.registerTempTable("drama_genre")
    
    drama_movies = spark.sql("select b._c1 as genre, a._c1 as title, a._c2 as summary, year(a._c3) as prod_year from movies as a inner join drama_genre as b on a._c0 = b._c0")
    drama_movies.registerTempTable("drama_movies") #[genre, title, summary, prod_year]
    #drama_movies.show()
    
    drama_movies_after_2K_lustrum_sum_len = spark.sql("select groupLustrum(prod_year) as lustrum, char_length(summary) as sum_len from drama_movies where prod_year >= 2000")
    drama_movies_after_2K_lustrum_sum_len.registerTempTable("drama_movies_after_2K_lustrum_sum_len") #[pentaetia, summary_length]
    
    total_summaries_per_5_years = spark.sql("select lustrum, count(*) as num_summaries from drama_movies_after_2K_lustrum_sum_len group by lustrum")
    total_summaries_per_5_years.registerTempTable("total_summaries_per_5_years") #[pentaetia, total_movies_of_lustrum]
    
    sum_summaries_per_5_years = spark.sql("select lustrum, sum(sum_len) as sum_summaries from drama_movies_after_2K_lustrum_sum_len group by lustrum")
    sum_summaries_per_5_years.registerTempTable("sum_summaries_per_5_years") #[pentaetia, sum_of_all_lengths]
    
    average_summaries_per_5_years = spark.sql("with staging as (select a.lustrum, a.sum_summaries, b.num_summaries from sum_summaries_per_5_years as a inner join total_summaries_per_5_years as b on (a.lustrum = b.lustrum)) select lustrum, (sum_summaries/num_summaries) as average_summary from staging")
    average_summaries_per_5_years.show(50) #[pentaetia, average_length]
    
    
    #resultFile.close()

    spark.stop()