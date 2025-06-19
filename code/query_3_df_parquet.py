from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

username = "username"  # π.χ. georgiannarenta

spark = SparkSession.builder.appName("Query3_DataFrame").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query3_df_output_{job_id}"

# Φόρτωση των δεδομένων από parquet
pop_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/2010_Census_Populations_by_Zip_Code.parquet")
income_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_income_2015.parquet")

# Join στα Zip Code
joined_df = pop_df.alias("p").join(
    income_df.alias("i"),
    col("p.Zip Code") == col("i.Zip Code")
).select(
    col("p.Zip Code"),
    col("p.Average Household Size"),
    col("i.Estimated Median Income")
)

# Υπολογισμός εισοδήματος ανά άτομο
result_df = joined_df.filter(
    (col("Average Household Size").isNotNull()) & (col("Average Household Size") != 0) &
    (col("Estimated Median Income").isNotNull())
).withColumn(
    "Income_per_Person",
    round(col("Estimated Median Income") / col("Average Household Size"), 6)
).select(
    col("Zip Code"),
    col("Income_per_Person")
)

# Ταξινόμηση (προαιρετικό)
result_df = result_df.orderBy(col("Income_per_Person").desc())


result_df.show()

# Αποθήκευση αποτελεσμάτων
result_df.write.mode("overwrite").parquet(output_dir)
