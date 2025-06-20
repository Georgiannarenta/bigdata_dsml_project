from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

username = "username" #georgiannarenta,ioannisanagnostaras
spark = SparkSession.builder.appName("Query 1 DataFrame UDF").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query1_df_udf_output_{job_id}"

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
def age_group(age):
    try:
        age = int(age)
    except:
        return None
    if age < 18:
        return "Παιδιά"
    elif 18 <= age <= 24:
        return "Νεαροί ενήλικοι"
    elif 25 <= age <= 64:
        return "Ενήλικοι"
    else:
        return "Ηλικιωμένοι"

age_group_udf = udf(age_group, StringType())

result = (
    crime_data_df
    .withColumn("AgeGroup", age_group_udf(col("Vict Age")))
    .groupBy("AgeGroup")
    .count()
    .orderBy(col("count").desc())
)

result.show()
