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
      print("Usage: q2_sql_csv <file>", file=sys.stderr)
      exit(-1)
    
    #resultFile= open("resultFile_q2_sql_csv.txt","w+")    
    
    spark = SparkSession\
      .builder\
      .appName("q2_sql_csv")\
      .getOrCreate()

    #sc = spark.sparkContext   

    
    inputRatings = spark.read.format('csv').options(header='false', inferSchema='true').load(sys.argv[1])
    inputRatings.registerTempTable("ratings")
    #spark.sql("select _c3, _c5, _c6 from movies order by _c3 desc").show(30)
    
    total_ratings_per_user = spark.sql("select _c0 as user_id, count(*) as ratings_user from ratings group by user_id")
    total_ratings_per_user.registerTempTable("total_ratings_per_user") #[user_id, total_ratings]
    
    sum_ratings_per_user = spark.sql("select _c0 as user_id, sum(_c2) as sum_ratings_user from ratings group by user_id")
    #sum_ratings_per_user.show()
    sum_ratings_per_user.registerTempTable("sum_ratings_per_user") #[user_id, sum_ratings]
    
    average_ratings_per_user = spark.sql("with staging as (select a.user_id, a.sum_ratings_user, b.ratings_user from sum_ratings_per_user as a inner join total_ratings_per_user as b on (a.user_id = b.user_id)) select user_id, (sum_ratings_user/ratings_user) as average_ratings_user from staging order by user_id")
    #average_ratings_per_user.show(50)
    average_ratings_per_user.registerTempTable("average_ratings_per_user") #[user_id, average_rating]
    
    users_above_3 = spark.sql("select * from average_ratings_per_user where float(average_ratings_user) > float(3)")
    users_above_3.registerTempTable("users_above_3")
    
    total_users = spark.sql("select count(*) from average_ratings_per_user")
    total_users.show()
    
    
    total_users_above_3 = spark.sql("select count(*) from users_above_3")
    total_users_above_3.show()
    
    
    #resultFile.close()

    spark.stop()
