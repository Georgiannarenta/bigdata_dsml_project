from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, regexp_replace

username = "username"  #georgiannarenta,ioannisanagnostaras

spark = SparkSession.builder.appName("Query3_CSV").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query3_csv_output_{job_id}"

pop_df = spark.read.csv(
    "hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv",
    header=True,
    inferSchema=True
)

income_df = spark.read.csv(
    "hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv",
    header=True,
    inferSchema=True
)

income_clean_df = income_df.withColumn(
    "Estimated_Median_Income_Clean",
    regexp_replace(regexp_replace(col("Estimated Median Income"), "\\$", ""), ",", "").cast("double")
)


pop_df = pop_df.withColumn("Zip Code", col("Zip Code").cast("string")) \
               .withColumn("Average Household Size", col("Average Household Size").cast("double"))

income_clean_df = income_clean_df.withColumn("Zip Code", col("Zip Code").cast("string"))

pop_filtered_df = pop_df.filter(col("Average Household Size").isNotNull() & (col("Average Household Size") > 0))
income_filtered_df = income_clean_df.filter(col("Estimated_Median_Income_Clean").isNotNull() & (col("Estimated_Median_Income_Clean") > 0))


joined_df = pop_filtered_df.alias("p").join(
    income_filtered_df.alias("i"),
    col("p.Zip Code") == col("i.Zip Code")
).select(
    col("p.Zip Code"),
    col("p.Average Household Size"),
    col("i.Estimated_Median_Income_Clean").alias("Estimated Median Income")
)

result_df = joined_df.withColumn(
    "Income_per_Person",
    round(col("Estimated Median Income") / col("Average Household Size"), 6)
).select(
    col("Zip Code"),
    col("Income_per_Person")
)

result = result_df.collect()

for item in result:
    print(item)
    
result_df.write.mode("overwrite").csv(output_dir, header=True)
