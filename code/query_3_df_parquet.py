from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, regexp_replace

username = "username"  # georgiannarenta, ioannisanagnostaras

spark = SparkSession.builder.appName("Query3_DataFrame_FilterDollar").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query3_df_output_{job_id}"

pop_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/2010_Census_Populations_by_Zip_Code.parquet")
income_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_income_2015.parquet")

# Φιλτράρεις τα εισοδήματα που ξεκινούν με $
income_filtered_df = income_df.filter(col("Estimated Median Income").startswith("$"))

# Καθαρίζεις τα πεδία εισοδήματος αφαιρώντας $ και κόμματα, και κάνεις cast σε double
income_clean_df = income_filtered_df.withColumn(
    "Estimated_Median_Income_Clean",
    regexp_replace(regexp_replace(col("Estimated Median Income"), "\\$", ""), ",", "").cast("double")
)

# Κάνεις join με τα δεδομένα πληθυσμού
joined_df = pop_df.alias("p").join(
    income_clean_df.alias("i"),
    col("p.Zip Code") == col("i.Zip Code")
).select(
    col("p.Zip Code"),
    col("p.Average Household Size"),
    col("i.Estimated_Median_Income_Clean").alias("Estimated Median Income")
)

# Υπολογίζεις το εισόδημα ανά άτομο
result_df = joined_df.withColumn(
    "Income_per_Person",
    round(col("Estimated Median Income") / col("Average Household Size"), 6)
).select(
    col("Zip Code"),
    col("Income_per_Person")
).orderBy(col("Income_per_Person").desc())

result_df.show()

# Αποθηκεύεις το αποτέλεσμα
result_df.write.mode("overwrite").parquet(output_dir)


