from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

username = "username" #georgiannarenta,ioannisanagnostaras
spark = SparkSession.builder.appName("Query 1 DataFrame No UDF").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query1_df_no_udf_output_{job_id}"

crime_data_10_19 = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2010_2019.parquet"
).select("Crm Cd Desc", "Vict Age")

crime_data_20_25 = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2020_2025.parquet"
).select("Crm Cd Desc", "Vict Age")

crime_data_df = crime_data_10_19.unionByName(crime_data_20_25)

crime_data_df = crime_data_df.filter(
    (col("Crm Cd Desc").contains("AGGRAVATED ASSAULT")) &
    (col("Vict Age").isNotNull())
)

crime_data_df = crime_data_df.withColumn(
    "AgeGroup",
    when(col("Vict Age") < 18, "Παιδιά")
    .when((col("Vict Age") >= 18) & (col("Vict Age") <= 24), "Νεαροί ενήλικοι")
    .when((col("Vict Age") >= 25) & (col("Vict Age") <= 64), "Ενήλικοι")
    .otherwise("Ηλικιωμένοι")
)

result = (
    crime_data_df
    .groupBy("AgeGroup")
    .count()
    .orderBy(col("count").desc())
)

result.show()
result.write.mode("overwrite").parquet(output_dir)
