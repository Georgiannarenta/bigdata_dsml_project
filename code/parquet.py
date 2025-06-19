from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("Convert CSV to Parquet") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "username" #georgiannarenta #ioannisanagnostaras

job_id = sc.applicationId
output_dir = f'hdfs://hdfs-namenode:9000/user/{username}/parquet_convert_output_{job_id}'

base_input_path = "hdfs://hdfs-namenode:9000/user/root/data/"
base_output_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/"

# Λίστα αρχείων CSV 
csv_files = [
    "LA_Crime_Data_2010_2019.csv",
    "LA_Crime_Data_2020_2025.csv",
    "LA_Police_Stations.csv",
    "LA_income_2015.csv",
    "2010_Census_Populations_by_Zip_Code.csv"
]

# Μετατροπή CSV σε Parquet
for file in csv_files:
    df = spark.read.option("header", "true").csv(base_input_path + file)
    df.write.mode("overwrite").parquet(base_output_path + file.replace(".csv", ".parquet"))

mo_codes_path = base_input_path + "MO_codes.txt"
mo_rdd = spark.sparkContext.textFile(mo_codes_path)
mo_df = mo_rdd.map(lambda line: line.strip().split(' ', 1)) \
              .filter(lambda parts: len(parts) == 2) \
              .toDF(["code", "description"])
mo_df.write.mode("overwrite").parquet(base_output_path + "MO_codes.parquet")
