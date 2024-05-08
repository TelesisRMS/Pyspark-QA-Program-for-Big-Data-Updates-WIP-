"""
This program is a WIP. 

Several of our big data clients submit regular, usually monthly, updates for my team to clean and load into AWS.  The QA approach used in this
effort is to summarize each update and then compare the counts and other statistics from update to update looking for variances outside of 
expected bounds.

This software is currently calculating statistics for updates and saving stats in s3 buckets for future analysis.  It also reads in stats for
two different snapshots and does simple comparisons which are useful but the goal is to automate these efforts and generate exception reports.
"""

"""
WIP program to QA client updates to analytical hive tables in AWS.
"""

from pyspark.sql  import SparkSession
from pyspark.conf import SparkConf

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.types import DateType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, LongType
import datetime
from dateutil.relativedelta import relativedelta, SU, MO
from datetime import date, timedelta
import time
import itertools
from pyspark.sql.utils import AnalysisException

import os.path
from os import path

"""

CAUTION - OUTPUT IS SET TO OVERWRITE


"""
#############################################################################
# Read data
#############################################################################
def readdata(input_location,partition_name):
    df1 = spark.read.parquet(input_location)
    print(input_location)
    print("Row count ",df1.count())
    df1selectinfocount=df1.groupBy(partition_name).count()
    df1selectinfocount.show() 
 
#############################################################################
# Create a partition for stat generation (create test data)
#############################################################################
def createpartition(input_location,output_location,partition_name,partition):
   
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
       
    df1 = spark.read.parquet(input_location)
    print(input_location)
    print("Total row count ",df1.count())

    listinfo=[partition]
    # -> need to figure out to have filter column based on parm
    df1selectinfo=df1.filter(df1.per_num.isin(listinfo))
    df1selectinfocount=df1selectinfo.groupBy(partition_name).count()
    df1selectinfocount.show()

    df1selectinfo.coalesce(1).write.mode('overwrite').partitionBy(partition_name).parquet(output_location)
    print("Done")

#############################################################################
# Read partition data ( test data)
#############################################################################
def readpartition(input_location,partition_name,partition):
    input_location=input_location+"/"+partition_name+"="+partition+"/"

    df1 = spark.read.parquet(input_location)
    print(input_location)
    print("Row count ",df1.count())
   
    df1.printSchema()
   
    print("Done")  

#############################################################################
# Accumulate stats for partition
#############################################################################
def accumulate1(input_analysis_data,partition_name,partition,input_stats_data):
   
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
   
    input_location=input_analysis_data+"/"+partition_name+"="+partition+"/"
    output_location=input_stats_data

    df1 = spark.read.parquet(input_location)
    print(input_location)
    print("Row count ",df1.count())
    
# Generate total counts
    
    df1count=df1.select([f.count(f.col(c)).alias(c) for c in df1.columns])
    df1count=df1count.withColumn("QA_Metric",f.lit("Count"))

    print("Count")
    df1count.show()

# Generate minimum
    
    df1min=df1.select([f.min(f.col(c)).alias(c) for c in df1.columns])
    df1min=df1min.withColumn("QA_Metric",f.lit("Minimum"))

    print("Minimum")
    df1min.show()
   
# Generate minimum
    
    df1max=df1.select([f.max(f.col(c)).alias(c) for c in df1.columns])
    df1max=df1max.withColumn("QA_Metric",f.lit("Maximum"))

    print("Maximum")
    df1max.show()

 
# Generate missing counts
    
    df1missing = df1.select([f.count(f.when(f.col(c).contains('None') | \
                            f.col(c).contains('NULL') | \
                            (f.col(c) == '' ) | \
                            f.col(c).isNull() | \
                            f.isnan(c), c
                           )).alias(c)
                    for c in df1.columns])
    df1missing=df1missing.withColumn("QA_Metric",f.lit("Missing"))
       
    print("Missing")
    df1missing.show()
   
    stat1=df1count.union(df1missing)
    stat2=stat1.union(df1min)
    statistics=stat2.union(df1max)
    statistics=statistics.withColumn(partition_name,f.lit(partition))
    statistics.show()
    print("Done accumulating statistics")

    statistics.coalesce(1).write.mode('overwrite').partitionBy(partition_name).parquet(output_location)
   
    print("Done writing out statistics")  
 
#############################################################################
# Read statsitics
#############################################################################
def readstats(input_stats_data,partition_name,partition):
    input_location=input_stats_data+"/"+partition_name+"="+partition+"/"
   
    df1 = spark.read.parquet(input_location)
    print(input_location)
    print("Row count ",df1.count())
   
    df1.show()
    df1.printSchema()
   
    print("Done")  
 
def pathexist(s3location):
    print(s3location)
    try:
        df1 = spark.read.parquet(s3location)
        return True
    except:
        return False

def findpartitionsdate():
   
    partitionlist=[]
       
    s3location="s3://expn-cis-data-engg-ingestion-output-edo/loadqa/version1/date={}/"
    s3locationinclpartition=""
   
    dstart=date(2023,4,1)
    for x in range(40):
        datestring=dstart.strftime("%m-%d-%Y")
        s3locationinclpartition=s3location.format(datestring)
        if pathexist(s3locationinclpartition):
            print("Bingo")
            partitionlist.append(datestring)
        dstart=dstart+timedelta(days=1)
       
    print("number of partitions ",len(partitionlist))
    print(partitionlist[0],partitionlist[1])
   
    return partitionlist

def findpartitionspernum(input_location):
   
    partitionlist=[]
       
    s3location=input_location+"/pernum={}/"
    s3locationinclpartition=""
   
    pernumpartition=6430
    for x in range(40):
        pernumpartition+=1
        s3locationinclpartition=s3location.format(str(pernumpartition))
#        print(len(s3locationinclpartition),"["+s3locationinclpartition+"]")
        if pathexist(s3locationinclpartition):
            print("Bingo")
            partitionlist.append(pernumpartition)
        else:
            print("Hmm ",s3locationinclpartition)
       
    print("number of partitions ",len(partitionlist))
#    print(partitionlist[0],partitionlist[1])
   
    return partitionlist

def createDVR(stat_location,partition_name):
   
#    partitionlist=findpartitions()
    partitionlist=[6440,6441]

    print("number of partitions ",len(partitionlist))
    print(partitionlist[0],partitionlist[1])

    print("===============================")
    print("Two sets of stats")
    print("===============================")
   
    input_location=stat_location+"/"+partition_name+"="+str(partitionlist[0])+"/"
   
    df1first = spark.read.parquet(input_location)
    print(input_location)
    print("Row count ",df1first.count())

    df1first=df1first.orderBy('QA_Metric')
    df1first.show()

    input_location=stat_location+"/"+partition_name+"="+str(partitionlist[1])+"/"
   
    df1second = spark.read.parquet(input_location)
    print(input_location)
    print("Row count ",df1second.count())
   
    df1second=df1second.orderBy('QA_Metric')
    df1second.show()
    df1fn=df1first.schema.fieldNames()

    numvartoanalyze=len(df1first.columns)
    numvartoanalyze=20
    
    print("===============================")
    print("Two sets of stats")
    print("===============================")
   
    print("Ascend Data Operations Data Validation Report")
    print("---------------------------------------------")
    print("Run date",date.today())
    print("")
    print(partitionlist[0],"vs",partitionlist[1])
   
    print("")
    print("Counts")
    for x in range(numvartoanalyze):
        print(x,df1fn[x],df1first.collect()[0][x],"versus",df1second.collect()[0][x])
   
    print("")
    print("Missing")
    for x in range(numvartoanalyze):
        print(x,df1fn[x],df1first.collect()[3][x],"versus",df1second.collect()[3][x])
   
    print("")
    print("Minimum")
    for x in range(numvartoanalyze):
        print(x,df1fn[x],df1first.collect()[2][x],"versus",df1second.collect()[2][x])
   
    print("")
    print("Maximum")
    for x in range(numvartoanalyze):
        print(x,df1fn[x],df1first.collect()[1][x],"versus",df1second.collect()[1][x])
       
    
    
    print("")
    print("Done")  
 
   
#############################################################################   
# Begin program
#############################################################################

partition_name="per_num"
partition="6440"
input_all_data="s3://expn-cis-data-engg-ingestion-output-edo/qatestdata/performance_vintage"
input_analysis_data="s3://expn-cis-data-engg-ingestion-output-edo/loadqa/test1"
input_stats_data="s3://expn-cis-data-engg-ingestion-output-edo/loadqa/stats/version1"


#readdata(input_all_data,partition_name)
#createpartition(input_all_data,input_analysis_data,partition_name,partition)
#readpartition(input_analysis_data,partition_name,partition)   
#accumulate1(input_analysis_data,partition_name,partition,input_stats_data)
#readstats(input_stats_data,partition_name,partition)
#findpartitionspernum(input_stats_data)
createDVR(input_stats_data,partition_name)

