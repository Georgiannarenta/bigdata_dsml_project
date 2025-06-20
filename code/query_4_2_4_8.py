from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, lower, upper, radians, sin, cos, atan2, sqrt, pow, count, avg
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
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
meters_per_degree_lat = 111320
feet_to_meters = 0.3048

crime_weapons = crime_weapons.withColumn(
    "AREA_NAME_UPPER",
    upper(col("AREA NAME"))
)

police_df = police_df.withColumn("lat_deg", (col("Y") * feet_to_meters) / meters_per_degree_lat)
police_df = police_df.withColumn("lon_deg", (col("X") * feet_to_meters) / (meters_per_degree_lat * math.cos(math.radians(34))))
police_df = police_df.withColumn("DIVISION_UPPER", upper(col("DIVISION")))

joined_df = crime_weapons.join(police_df, crime_weapons["AREA_NAME_UPPER"] == police_df["DIVISION_UPPER"])
joined_df = joined_df.withColumn("dlat", radians(col("lat_deg") - col("LAT"))) \
                     .withColumn("dlon", radians(col("lon_deg") - col("LON"))) \
                     .withColumn("lat1", radians(col("LAT"))) \
                     .withColumn("lat2", radians(col("lat_deg")))

joined_df = joined_df.withColumn("a", 
    pow(sin(col("dlat") / 2),2) + 
    cos(col("lat1")) * cos(col("lat2")) * 
    pow(sin(col("dlon") / 2),2)
)

joined_df = joined_df.withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1 - col("a"))))

joined_df = joined_df.withColumn("distance", col("c") * R)

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

