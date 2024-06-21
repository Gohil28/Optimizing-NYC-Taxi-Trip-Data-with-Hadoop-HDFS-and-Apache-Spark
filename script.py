from pyspark.sql.functions import *
from pyspark.sql.types import *

#Load data and PrintSchema 
df = spark.read.parquet("hdfs://localhost:9000/project_data/yellow_tripdata_2024-01.parquet", header=True, inferSchema=True)
df.printSchema()

#Selected Columns transfer to new data frame 
my_df = df.select(col('VendorID'),col('tpep_pickup_datetime'),col('tpep_dropoff_datetime'),col('passenger_count'),col('trip_distance'),col('RatecodeID'),col('payment_type'),col('fare_amount'),col('tip_amount'),(col('extra')+col('mta_tax')+col('tolls_amount')+col('improvement_surcharge')+col('congestion_surcharge')).alias('Other_amount'),col('total_amount'))
my_df.show(5)

#clean Dataframe 
my_df = my_df.dropna()
my_df = my_df.dropDuplicates()
my_df = my_df.filter((col("trip_distance") > 0) & (col("fare_amount") > 0))
my_df = my_df.distinct()

#Change column name and Type 
my_df = my_df.withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime")).withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))

my_df = my_df.withColumn("pickup_datetime",col("tpep_pickup_datetime")).withColumn("dropoff_datetime",col("tpep_dropoff_datetime"))

#Drop Extra Column
column_to_drop = ['tpep_pickup_datetime','tpep_dropoff_datetime']
my_df = my_df.drop(*column_to_drop)
my_df.printSchema()


#Make Changes in datatypes 
double_columns = ["trip_distance", "fare_amount", "tip_amount", "Other_amount", "total_amount"]

for col_name in double_columns:
	my_df = my_df.withColumn(col_name,round(col(col_name),2))

#Put clean_df file to hdfs in parquet format with repartition 1 for 1 file

clean_df = clean_df.repartition(1)
clean_df.write.mode("overwrite").option("header", "true").parquet('hdfs://localhost:9000/project_data/clean_data_yellow_tripdata_parquet_repartition')

#Put clean_df file to hdfs in parquet format without repartition 
clean_df.write.mode("overwrite").option("header", "true").parquet('hdfs://localhost:9000/project_data/clean_data_yellow_tripdata_parquet')

#Get clean data  from hdfs and read in pyspark for next task
new_df =spark.read.parquet("hdfs://localhost:9000/project_data/clean_data_yellow_tripdata_parquet",header=True,inferSchema=True)

new_df.count()
2754462


#Basic Analysis and Exploration

# Q1.  Sum of total amout by different payment types 

result_df = new_df.groupBy('payment_type').agg(count('payment_type').alias('count'), sum('total_amount').alias('total_sum'))
formatted_df = result_df.withColumn('total_sum', format_number(col('total_sum'), 2))
formatted_df.show()


# Q2. Find all trips where the tip_amount was greater than 15% of the fare_amount
high_tips_trip = new_df.filter(col('tip_amount')> 0.15 * col('total_amount'))
high_tips_trip.show(5)

#Q3. Busiest hour of the day based on ‘pickup_datetime’ top 5 :
busy_hour = new_df.groupBy(hour('pickup_datetime').alias('hour')).count().sort(col('count').desc())
busy_hour.show(5)

#Q4. Find avg duration by all week days 
trip_duration = new_df.withColumn('Duration',(unix_timestamp(col('dropoff_datetime')) - unix_timestamp(col('pickup_datetime')))/60)
avg_duration_by_day = trip_duration.groupBy(dayofweek('pickup_datetime').alias('day_of_week')).agg(avg('Duration').alias('Avg_duration'))
avg_duration_by_day.show()

#Q5.Trips taken on Morning , Afternoon and Night time count
trips_by_interval = new_df.withColumn('time_interval',when((hour('pickup_datetime') >= 0) & (hour('pickup_datetime') < 6), 'Night').when((hour('pickup_datetime') >= 6) & (hour('pickup_datetime') < 12), 'Morning').when((hour('pickup_datetime') >= 12) & (hour('pickup_datetime') < 18), 'Afternoon').otherwise('Evening')).groupBy('time_interval').count().sort(col('count').desc())
trips_by_interval.show()


# Now we are get data from hdfs (10% of data from this data ) and move to local system
#Make data small 20% 
small_df = new_df.sample(fraction=0.2, seed=42)
small_df.show(10)

#Put small data to hdfs in CSV format
#small_df.write.csv('hdfs://localhost:9000/project_data/small_data',header=True)

#Now working with SQl with SparkSql 
small_df.createOrReplaceTempView('taxi_data')
spark.sql(""" select VendorId,passenger_count,trip_distance,payment_type,fare_amount,tip_amount,Other_amount,total_amount,pickup_datetime,dropoff_datetime from taxi_data""").show(5)

#Now Partition data by trip duration category for better data performance and efficiency
data1 = small_df.withColumn('trip_duration_min', round((unix_timestamp('dropoff_datetime') - unix_timestamp('pickup_datetime')) / 60, 2))
data1 = data1.withColumn("trip_category", when(col("trip_duration_min") < 10, "short").when((col("trip_duration_min") >= 10) & (col("trip_duration_min") < 30), "mid").otherwise("long"))
data1.show(5)


#Put this data on hdfs 
>>>data1.write.partitionBy("trip_category").parquet("hdfs://localhost:9000/project_data/partition_trip_category")

#Create new table by from this data 
partition_trip = spark.sql(""" create table data_partitioned using PARQUET PARTITIONED BY (trip_category) AS SELECT VendorID, passenger_count, trip_distance, payment_type, total_amount, pickup_datetime, dropoff_datetime, trip_duration_min, trip_category    FROM taxi_data_temp """)

partition_trip = spark.sql(""" select * from taxi_data_temp """)
partition_trip.show(5)

