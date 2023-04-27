# Importing Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import ast
import functools

# Creation of Spark Session
spark = SparkSession \
        .builder \
        .appName("Structured Streaming ")\
        .enableHiveSupport()\
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
df = spark \
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","18.211.252.152:9092")\
    .option("subscribePattern","transactions-topic-verified.*")\
    .option("startingOffsets", "earliest") \
    .load()

# Take set of SQL expressions in a string to execute
df1 = df.selectExpr("CAST(value AS STRING)")

# Functions to get data from kafka in appropriate fields
def card_read(a):
    b = ast.literal_eval(a)
    return b['card_id']
def member_read(a):
    b = ast.literal_eval(a)
    return b['member_id']
def amount_read(a):
    b = ast.literal_eval(a)
    return b['amount']
def postcode_read(a):
    b = ast.literal_eval(a)
    return b['postcode']
def pos_read(a):
    b = ast.literal_eval(a)
    return b['pos_id']
def txn_read(a):
    b = ast.literal_eval(a)
    return b['transaction_dt']

# Defining UDFs for putting datas in particular fields
cardRead = udf(lambda a: card_read(a),StringType())
df2 = df1.withColumn('card_id',cardRead(df1.value))

memRead = udf(lambda a: member_read(a),StringType())
df3 = df2.withColumn('member_id', memRead(df2.value))

amtRead = udf(lambda a: amount_read(a),StringType())
df4 = df3.withColumn('amount', amtRead(df3.value))

postRead = udf(lambda a: postcode_read(a),StringType())
df5 = df4.withColumn('postcode', postRead(df4.value))

posRead = udf(lambda a: pos_read(a),StringType())
df6 = df5.withColumn('pos_id', posRead(df5.value))

txnRead = udf(lambda a: txn_read(a),StringType())
df7 = df6.withColumn('transaction_dt', txnRead(df6.value))

# Dropping the value column as the value is extracted from value
df8 = df7.drop('value')

# Typecasting amount to Double Type
df8.withColumn('amount',df8.amount.cast(DoubleType()))

# Reading the uszipsv csv and creating a dataframe for the same
df_zip = spark.read.csv('s3://cardtransac/uszipsv.csv')
df_zip = df_zip.withColumnRenamed('_c0','postcode').withColumnRenamed('_c1','lat').withColumnRenamed('_c2','long')

# Typecasting latitide and longitude to DoubleType
df_zip = df_zip.withColumn('lat',df_zip.lat.cast(DoubleType())).withColumn('long',df_zip.long.cast(DoubleType()))

# Connecting to hive database
spark.sql('use capstone_project')
df9 = spark.sql("select card_id,ucl,score from lookup_data_hbase_orc")

# Joining the kafka stream with lookup data to get the UCL and the Credit Score
df10=df8.join(df9,'card_id','left_outer')

# Joining the updated joined dataset to get the lattitude and longitude of Postcode
df14=df10.join(df_zip,'postcode','left_outer')

# Dropping irrelevant columns
df14 = df14.drop('_c3')
df14 = df14.drop('_c4')

# Renaming Columns
df14 = df14.withColumnRenamed('transaction_dt','transaction_dt_current').withColumnRenamed('lat','lat_current').withColumnRenamed('long','long_current').withColumnRenamed('postcode','postcode_current')

# Reading data from hive
df15 = spark.sql("select card_id,postcode,transaction_dt from lookup_data_hbase_orc")

#Joining dataframes to get another set of lattitude and longitude for the Postcode for last transaction happened present in lookup
df16 = df14.join(df15,'card_id','left_outer')
df17 = df16.join(df_zip,'postcode','left_outer')

# Dropping irrelevant columns
df17 = df17.drop('_c3')
df17 = df17.drop('_c4')

# Renaming Columns
df17 = df17.withColumnRenamed('transaction_dt','transaction_dt_lookup').withColumnRenamed('lat','lat_lookup').withColumnRenamed('long','long_lookup').withColumnRenamed('postcode','postcode_lookup')

# Typecasting Current Transaction column to TimeStamp type
df17 = df17.withColumn('transaction_dt_current',unix_timestamp(df17.transaction_dt_current,'dd-MM-yyyy HH:mm:ss').cast("timestamp"))
df17 = df17.withColumn('transaction_dt_lookup',unix_timestamp(df17.transaction_dt_lookup,'dd-MM-yyyy HH:mm:ss').cast("timestamp"))
import math

# Defining function for calculation of distance between two points
def dist(lat1, lon1, lat2, lon2):
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2*math.asin(math.sqrt(a))
    r = 6371

    return (c*r)

# Applying UDF in distance column
distance = udf(dist, DoubleType())
df17 = df17.withColumn('Distance',distance('lat_current','long_current','lat_lookup','long_lookup'))

# Calculation of difference between current and reference timestamp
df18 = df17.withColumn('diffInSeconds',col('transaction_dt_current').cast(DoubleType())-col('transaction_dt_lookup').cast(DoubleType()))
df18 = df18.withColumn('diffInHours',round(col('diffInSeconds')/3600))
df18 = df18.withColumn('Speed',col('Distance')/col('diffInHours'))

# Classifying Fraud and Genuine
union_df = df18.withColumn("status",when((col('amount')< col('ucl'))& (col('score') > 200) & (col('Speed') <= 900.0),'Genuine')
        .when((col('amount')>=col('ucl'))| (col('score') <= 200) | (col('Speed') > 900.0),'Fraud'))


# Dropping irrelevant columns
card_txn = union_df.drop('ucl')
card_txn = card_txn.drop('score')
card_txn = card_txn.drop('diffInSeconds')
card_txn = card_txn.drop('lat_current')
card_txn = card_txn.drop('long_current')
card_txn = card_txn.drop('lat_lookup')
card_txn = card_txn.drop('long_lookup')
card_txn = card_txn.drop('transaction_dt_lookup')
card_txn = card_txn.drop('postcode_lookup')
card_txn = card_txn.drop('Distance')
card_txn = card_txn.drop('diffInHours')

# Renaming the columns and making the data rearranged in proper format
card_txn = card_txn.withColumnRenamed('transaction_dt_current','transaction_dt').withColumnRenamed('postcode_current','postcode')
card_txn = card_txn.select('card_id','member_id','amount','postcode','pos_id','transaction_dt','status')

# Query to show data in the console
query = card_txn\
        .writeStream\
        .outputMode("append")\
        .option("truncate", "false") \
        .format("console")\
        .start()

# Query Terminiation
query.awaitTermination()

