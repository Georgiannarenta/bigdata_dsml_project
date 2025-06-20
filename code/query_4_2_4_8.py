from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, lower, upper, udf, sqrt, pow, count, avg , radians
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import DoubleType
import math 

username = 'username' #georgiannarenta, ioannisanagnostaras
spark = (
    SparkSession.builder
    .appName("Query4_2_4_8")
    .config("spark.executor.instances", "2") 
    .config("spark.executor.cores", "4")     
    .config("spark.executor.memory", "8g")  
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query4_df_2_4_8_output_{job_id}"
crime_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2010_2019.parquet") \
           .union(spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2020_2025.parquet"))

police_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Police_Stations.parquet")
mo_codes_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/MO_codes.parquet")

crime_df = crime_df.withColumn("mo_code", explode(split(col("Mocodes"), " "))).filter(col("mo_code") != "")


crime_df = crime_df.filter((col("LAT") != 0) & (col("LON") != 0))


mo_codes_filtered = mo_codes_df.filter(
    (lower(col("description")).contains("gun")) | (lower(col("description")).contains("weapon"))
).select("code")

crime_weapons = crime_df.join(mo_codes_filtered, crime_df.mo_code == mo_codes_filtered.code, "inner").dropDuplicates(["DR_NO"])

R = 6371000 
lat0_rad = math.radians(34.0)  
transform_x = radians(col("LON")) * R * math.cos(lat0_rad)
transform_y =  radians(col("LAT")) * R
crime_weapons = crime_weapons.withColumn(
    "crime_x",
    transform_x
).withColumn(
    "crime_y",
    transform_y
).withColumn(
    "AREA_NAME_UPPER",
    upper(col("AREA NAME"))
)
feet_to_meters = 0.3048

police_df = police_df.withColumn("X", col("X") * feet_to_meters)
police_df = police_df.withColumn("Y", col("Y") *feet_to_meters)
police_df = police_df.withColumn("DIVISION_UPPER", upper(col("DIVISION")))


joined_df = crime_weapons.join(police_df, crime_weapons.AREA_NAME_UPPER == police_df.DIVISION_UPPER, "inner")

joined_df = joined_df.withColumn(
    "distance",
    sqrt(
        pow(col("crime_x") - col("X"), 2) +
        pow(col("crime_y") - col("Y"), 2)
    )
)


part = Window.partitionBy("DR_NO").orderBy(col("distance").asc())

closest_df = joined_df.withColumn("rn", row_number().over(part)).filter(col("rn") == 1)

result = closest_df.groupBy("DIVISION_UPPER") \
                   .agg(
                   count("*").alias("#"),
                   avg(col("distance") / 1000).alias("average_distance_km")
                   ) \
                   .orderBy(col("#").desc())


result.show(truncate=False)
result.write.mode("overwrite").parquet(output_dir)

