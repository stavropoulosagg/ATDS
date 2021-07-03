import sys

from pyspark.sql import SparkSession
from io import StringIO
import csv

if __name__ == "__main__":
    
    if len(sys.argv) != 3:
      print("Usage: csv_to_parquet <file>", file=sys.stderr)
      exit(-1)
    
    #resultFile= open("resultFile_q1_sql_csv.txt","w+")    
    
    spark = SparkSession\
      .builder\
      .appName("csv_to_parquet")\
      .getOrCreate()

    input_df = spark.read.format('csv').options(header='false', inferSchema='true').load(sys.argv[1])
    input_df.show()
    
    input_df.write.parquet(sys.argv[2])
    
    #resultFile.close()

    spark.stop()