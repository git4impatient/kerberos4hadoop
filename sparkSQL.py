from __future__ import print_function
import os, sys
#import path
from pyspark.sql import *

# create spark sql session
myspark = SparkSession\
    .builder\
    .config("spark.executor.instances", 3 ) \
    .config("spark.executor.memory", "5g") \
    .config("spark.executor.cores", 2) \
    .config("spark.dynamicAllocation.maxExecutors", 10) \
    .config("spark.scheduler.listenerbus.eventqueue.size", 10000) \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .appName("sparkSQL") \
    .getOrCreate()



sc = myspark.sparkContext

import time
print ( time.time())

sc.setLogLevel("ERROR")
print ( myspark )
# make spark print text instead of octal
myspark.sql("SET spark.sql.parquet.binaryAsString=true")

# read in the data file from HDFS
dfpfc = myspark.read.parquet ( "/user/hive/warehouse/sample_07p")
# you can also read directly from an s3 bucket, you would of course need the s3 IAM key 
# and permission to read the bucket
#dfpfc = myspark.read.parquet ( "s3a://impalas3a/sample_07_s3a_parquet")
# print number of rows and type of object
print ( dfpfc.count() )
print  ( dfpfc )

# create a table name to use for queries
dfpfc.createOrReplaceTempView("census07")
# run a query
fcout=myspark.sql('select * from census07 where salary > 100000')
fcout.show(5)
# create a dataframe with valid rows
mydf=myspark.sql('select code as txtlabel, salary, total_emp from census07 where total_emp > 0 and total_emp< 1000000 and salary >0 and salary<500000' )
mydf.show(5)

mydf.write.parquet ( "/tmp/mypysparkparquet")
