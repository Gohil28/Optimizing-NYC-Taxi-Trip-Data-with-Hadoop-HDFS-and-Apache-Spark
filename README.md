# Optimizing NYC Taxi Trip Data with Hadoop HDFS and Apache Spark

![one drawio](https://github.com/Gohil28/Optimizing-NYC-Taxi-Trip-Data-with-Hadoop-HDFS-and-Apache-Spark/assets/97522181/c6b93d8d-7d65-4340-8a33-bdc1e059bcfb)



This project delves into NYC taxi trip data, wielding the power of Apache Spark, a distributed processing framework, and its Python interface, PySpark. It guides you through a data analysis journey, uncovering insights through a series of steps:

1. **Data Acquisition:** PySpark collaborates with HDFS (Hadoop Distributed File System) to retrieve the data stored in Parquet format.

2. **Data Cleaning:** PySpark meticulously cleans the data, addressing missing values using `dropna()`, eliminating duplicates with `dropDuplicates()`, and filtering out irrelevant entries. Data types are also meticulously converted using `round()` for specific columns.

3. **Data Transformation:** PySpark restructures the data by selecting relevant columns, renaming them for clarity, and calculating new metrics like trip duration. This step sculpts the data for optimal analysis.

4. **Exploratory Analysis:** PySpark empowers you to explore the data through aggregations and visualizations. You can uncover:

   - Total trip amount by payment type.
   - Trips with high tips (exceeding 15% of the fare).
   - Busiest hours based on pickup time.
   - Average trip duration for each weekday.
   - Frequency of trips across morning, afternoon, evening, and night.

5. **Data Partitioning (Optional):** PySpark allows you to partition the data by trip duration categories ("short," "mid," or "long"). This enhances storage efficiency and facilitates faster retrieval of specific data subsets.

This project equips you with a comprehensive analysis of NYC taxi trip data, paving the way for further exploration and data-driven decision making.


Data Link : https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

 
