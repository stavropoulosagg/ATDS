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
      print("Usage: q1_sql_csv <file>", file=sys.stderr)
      exit(-1)
    
    #resultFile= open("resultFile_q1_sql_csv.txt","w+")    
    
    spark = SparkSession\
      .builder\
      .appName("q1_sql_csv")\
      .getOrCreate()

    #sc = spark.sparkContext   
      
    def computeProfit(x, y):
        revenue = int(y)
        cost = int(x)
        profit = (revenue - cost) / cost * 100
        return profit
    
    spark.udf.register("computeProfit", computeProfit)

    
    inputMovies = spark.read.format('csv').options(header='false', inferSchema='true').load(sys.argv[1])
    inputMovies.registerTempTable("movies")
    #spark.sql("select _c3, _c5, _c6 from movies order by _c3 desc").show(30)
    
    movie_year_profit = spark.sql("select _c1 as title, year(_c3) as year, computeProfit(_c5, _c6) as profit from movies where ((year(_c3) >= 2000) and (_c5 > 0) and (_c6 > 0))")
    movie_year_profit.registerTempTable("movie_year_profit") #[title, year, profit]
    
    max_profits = spark.sql("select year, max(float(profit)) as profit from movie_year_profit group by year")
    max_profits.registerTempTable("max_profits") #[year, max_profit]
    
    max_profit_movie_per_year = spark.sql("select a.year, a.title, a.profit from movie_year_profit as a inner join max_profits as b on (a.year = b.year and a.profit = b.profit) order by a.year")
    max_profit_movie_per_year.show(50)
    
    
    #resultFile.close()

    spark.stop()