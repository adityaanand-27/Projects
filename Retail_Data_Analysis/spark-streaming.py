# importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import ast

# Creating spark session
spark = SparkSession \
        .builder \
        .appName("Structured Streaming ")\
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Code to fetch the data from kafka server
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","18.211.252.152:9092")\
    .option("subscribe","real-time-project")\
    .load()

df1 = df.selectExpr("CAST(value AS STRING)")

# Code to define the schema of single order
schema = StructType()\
        .add("invoice_no",StringType())\
        .add("country",StringType())\
        .add("timestamp",TimestampType())\
        .add("type",StringType())\
        .add("items",StringType())

df2 = df1.select(from_json(col("value"),schema).alias("orders"))

df3 = df2.select("orders.*")

# Code to calculate the new UDFs and other utility function
def quantity(a):
    a = ast.literal_eval(a)
    quantity = 0
    for i in range (len(a)):
        quantity = quantity + a[i].get('quantity')
    return(quantity)

def price(a):
    a = ast.literal_eval(a)
    price = 0.0
    for i in range (len(a)):
        price = price + a[i].get('quantity')*a[i].get('unit_price')
    return(price)

def is_order(a):
    if a == "ORDER":
        return(1)
    else:
        return(0)

def is_return(a):
    if a == "RETURN":
        return(1)
    else:
        return(0)

def check_type(a,b):
    if a == 1:
        b = b * -1.0
    return(b)

quantityUDF = udf(lambda t:quantity(t),IntegerType())
priceUDF = udf(lambda t:price(t),DoubleType())
orderUDF = udf(lambda t:is_order(t),IntegerType())
returnUDF = udf(lambda t:is_return(t),IntegerType())
checkUDF = udf(lambda t,u:check_type(t,u),DoubleType())

df4 = df3.withColumn("total_items",quantityUDF(df3.items))
df5 = df4.withColumn("total_cost",priceUDF(df4.items))
df6 = df5.withColumn("is_order",orderUDF(df5.type))
df7 = df6.withColumn("is_return",returnUDF(df6.type))
df8 = df7.withColumn("total_cost",checkUDF(df7.is_return,df7.total_cost))
df9 = df8.select("invoice_no","country","timestamp","total_cost","total_items","is_order","is_return")

# Code to write final summarised input values to the console
query1 = df9 \
        .writeStream \
        .outputMode("append")\
        .format("console")\
        .option('truncate',False)\
        .trigger(processingTime = '1 minute')\
        .start()

# Code to calculate time based KPI with tumbling window of one minute
df10 = df9.withWatermark("timestamp","1 minute")\
        .groupBy(window('timestamp','1 minute','1 minute'))\
        .agg(sum('total_cost'),
                count('invoice_no').alias('OPM'),
                avg('is_return'))\
        .select('window',
                'OPM',
                format_number('sum(total_cost)',2).alias('total_sale_volume'),
                format_number(regexp_replace(format_number('sum(total_cost)',2),",","")/format_number('OPM',2),2).alias('average_transaction_size'),
                format_number('avg(is_return)',2).alias('rate_of_return'))

# Code to calculate country based KPI with tumbling window of one minute
df11 = df9.withWatermark("timestamp","1 minute")\
        .groupBy(window('timestamp','1 minute','1 minute'),'country')\
        .agg(sum('total_cost'),
                count('invoice_no').alias('OPM'),
                avg('is_return'))\
        .select('window',
                'country',
                'OPM',
                format_number('sum(total_cost)',2).alias('total_sale_volume'),
                format_number('avg(is_return)',2).alias('rate_of_return'))


query2 = df10\
        .writeStream \
        .outputMode("complete")\
        .format("console")\
        .option('truncate',False)\
        .trigger(processingTime = '1 minute')\
        .start()

query3 = df11 \
        .writeStream \
        .outputMode("complete")\
        .format("console")\
        .option('truncate',False)\
        .trigger(processingTime = '1 minute')\
        .start()

# Code to write time based KPI into one minute window each
query4 = df10\
        .writeStream\
        .format('json')\
        .outputMode("append")\
        .option('truncate',False)\
        .option('path','time-kpi')\
        .option('checkpointLocation','time-cp1')\
        .trigger(processingTime = '1 minute')\
        .start()

# Code to write country based KPI into one minute window each
query5 = df11\
        .writeStream\
        .format('json')\
        .outputMode("append")\
        .option('truncate',False)\
        .option('path','time-country-kpi')\
        .option('checkpointLocation','time-country-cp1')\
        .trigger(processingTime = '1 minute')\
        .start()


query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
query4.awaitTermination()
query5.awaitTermination()
