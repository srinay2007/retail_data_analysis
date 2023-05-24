# Importing required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Calculating Total_cost
def get_total_cost(items,type):
   total_cost = 0
   for item in items:
       total_cost = total_cost + (item[2] * item[3])
   if type == 'ORDER' or type == 'order' or type == 'Order':	
   	return total_cost
   else:
    	return -total_cost


# Calculating Total_Items
def get_total_item(items):
   total_items = 0
   for item in items:
       total_items = total_items + item[3]
   return total_items

# Getting order type flag
def get_is_order(type):
    order_type_flag = 0
    if type == 'ORDER' or type == 'order' or type == 'Order':
        order_type_flag = 1
    else:
        order_type_flag = 0
    return order_type_flag

# Gettting return type flag
def get_is_return(type):
    order_return_flag = 0
    if type == 'ORDER' or type == 'order' or type == 'Order':
        order_return_flag = 0
    else:
        order_return_flag = 1
    return order_return_flag

# Initializing Spark session
spark = SparkSession  \
        .builder  \
        .appName("KafkaRead")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Reading the streaming data
orderRaw = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","ec2-18-211-252-152.compute-1.amazonaws.com:9092")  \
        .option("subscribe","real-time-project")  \
        .load()

# Define the UDFs with UDF Function values
add_total_count = udf(get_total_item, IntegerType())
add_total_cost = udf(get_total_cost, DoubleType())
add_is_order_flag = udf(get_is_order, IntegerType())
add_is_return_flag = udf(get_is_return, IntegerType())

# Defining schema
jsonSchema = StructType() \
       .add("invoice_no", StringType()) \
       .add("country", StringType()) \
       .add("timestamp", TimestampType()) \
       .add("type", StringType()) \
       .add("items", ArrayType(StructType([
       StructField("SKU", StringType()),
       StructField("title", StringType()),
       StructField("unit_price", DoubleType()),
       StructField("quantity", IntegerType())
   ])))

# Parsing the Streaming data using from_json and schema
orderStream = orderRaw.select(from_json(col("value").cast("string"), jsonSchema).alias("data")).select("data.*")


# Deriving the Required new attributes using the UDF
Data_Frame_Total_Items_Cost= orderStream \
    .withColumn("Total_Items", add_total_count(orderStream.items)) \
    .withColumn("Total_Cost", add_total_cost(orderStream.items,orderStream.type)) \
    .withColumn("is_order", add_is_order_flag(orderStream.type)) \
    .withColumn("is_return", add_is_return_flag(orderStream.type)).select("invoice_no","country","timestamp","Total_Items", "Total_Cost","is_order", "is_return")

# Writing the Inetermediary data into Console
query = Data_Frame_Total_Items_Cost  \
        .writeStream  \
        .outputMode("append")  \
        .format("console")  \
        .option("truncate", "false") \
        .start()

# Calculating time based KPIs
aggStreamByTime = Data_Frame_Total_Items_Cost \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute", "1 minute")) \
    .agg(sum("Total_Cost").alias("total_volume_of_sales"),count("invoice_no").alias("OPM"),avg("is_return").alias("avg_rate_of_return"),avg("Total_Cost").alias("avg_transaction_size")).select("window", "OPM", "total_volume_of_sales", "avg_rate_of_return","avg_transaction_size")


# Calculating Country based KPIs 
aggStreamByCountry= Data_Frame_Total_Items_Cost \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute", "1 minute"), "country") \
    .agg(sum("Total_Cost").alias("total_volume_of_sales"),count("invoice_no").alias("OPM"),avg("is_return").alias("avg_rate_of_return")).select("window","country", "OPM", "total_volume_of_sales", "avg_rate_of_return")


# Writing the Time Based KPIs into HDFS location
queryByTime= aggStreamByTime.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "time-wise-kp1") \
    .option("checkpointLocation", "time-cp1") \
    .trigger(processingTime="1 minute") \
    .start()


# Writing the Country Based KPIs into HDFS location
queryByCountry = aggStreamByCountry.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "time-country-wise-kp1") \
    .option("checkpointLocation", "time-country-cp1") \
    .trigger(processingTime="1 minute") \
    .start()

# Setting up termination
queryByCountry.awaitTermination()
