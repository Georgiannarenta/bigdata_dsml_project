from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, when, count, round, row_number
from pyspark.sql.window import Window

username = "username" #georgiannarenta,ioannisanagnostaras

spark = SparkSession.builder.appName("Query2_DataFrame").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query_2_df_output_{job_id}"

crime_data_10_19 = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2010_2019.parquet"
).select("Date Rptd", "AREA NAME", "Status Desc")

crime_data_20_25 = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2020_2025.parquet"
).select("Date Rptd", "AREA NAME", "Status Desc")

crime_data = crime_data_10_19.unionByName(crime_data_20_25)

crime_data = crime_data.filter(
    col("Date Rptd").isNotNull() &
    col("AREA NAME").isNotNull() &
    col("Status Desc").isNotNull()
)

crime_data = crime_data.withColumn("year", year(to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a")))

crime_data = crime_data.withColumn("is_closed", when(col("Status Desc").isin("UNK", "Invest Cont"), 0).otherwise(1))

agg = crime_data.groupBy("year", "AREA NAME").agg(
    count("*").alias("total"),
    count(when(col("is_closed") == 1, True)).alias("closed")
).withColumn(
    "closed_case_rate",
    round((col("closed") / col("total")) * 100, 6)
).select(
    "year", col("AREA NAME").alias("precinct"), "closed_case_rate"
)

part = Window.partitionBy("year").orderBy(col("closed_case_rate").desc(), col("precinct"))

ranked = agg.withColumn("#", row_number().over(part))

top3 = ranked.filter(col("#") <= 3).orderBy("year", "#")

top3.show(truncate=False)

top3.write.mode("overwrite").parquet(output_dir)

