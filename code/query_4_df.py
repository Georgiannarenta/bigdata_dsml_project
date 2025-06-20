from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, lower, trim, broadcast, sqrt, pow, min as spark_min, avg, count
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
username = 'username' #georgiannarenta, ioannisanagnostaras
spark = SparkSession.builder \
    .appName("Query4_2_4_8") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query3_df_final_output_{job_id}"

# Φορτώνουμε τα parquet αρχεία (αν τα έχεις ήδη μετατρέψει)
crime_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2010_2019.parquet") \
            .union(spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2020_2025.parquet"))

police_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Police_Stations.parquet")

mo_codes_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/MO_codes.parquet")

crime_exp = crime_df.withColumn("mo_code", explode(split(col("mocodes"), " "))) \
                   .filter(col("mo_code") != "")

crime_exp = crime_exp.filter((col("latitude") != 0) & (col("longitude") != 0))

mo_codes_filtered = mo_codes_df.filter(
    (lower(col("description")).contains("gun")) | (lower(col("description")).contains("weapon"))
).select("code")

crime_weapons = crime_exp.join(mo_codes_filtered, crime_exp.mo_code == mo_codes_filtered.code, "inner")

crime_weapons = crime_weapons.withColumn("precinct_upper", upper(col("precinct")))


police_df = police_df.withColumnRenamed("latitude", "station_lat") \
                     .withColumnRenamed("longitude", "station_lon") \
                     .withColumn("division_upper", upper(col("division")))

def euclidean_distance(lat1_col, lon1_col, lat2_col, lon2_col):
    return sqrt(
        pow(lat1_col - lat2_col, 2) +
        pow(lon1_col - lon2_col, 2)
    )

cross_join_df = crime_weapons.crossJoin(broadcast(police_df))

cross_join_df = cross_join_df.withColumn(
    "distance",
    euclidean_distance(col("latitude"), col("longitude"), col("station_lat"), col("station_lon"))
)


window_spec = Window.partitionBy("crime_id").orderBy(col("distance").asc())

closest_df = cross_join_df.withColumn("rn", row_number().over(window_spec)) \
                          .filter(col("rn") == 1)

result = closest_df.groupBy("division_upper") \
    .agg(
        count("*").alias("#"),
        avg("distance").alias("average_distance")
    ) \
    .orderBy(col("#").desc())


result.show(truncate=False)

result.write.parquet(output_dir)

spark = SparkSession.builder \
    .appName("Query4_EuclideanDistance") \
    .config("spark.executor.instances", "2") \  # Χρησιμοποιούμε 2 executors
    .config("spark.executor.cores", "4") \  # Κάθε executor θα έχει 4 cores
    .config("spark.executor.memory", "8g") \  # Κάθε executor θα έχει 8GB μνήμης
    .getOrCreate()

spark = SparkSession.builder \
    .appName("Query4_EuclideanDistance") \
    .config("spark.executor.instances", "4") \  # Χρησιμοποιούμε 4 executors
    .config("spark.executor.cores", "2") \  # Κάθε executor θα έχει 2 cores
    .config("spark.executor.memory", "4g") \  # Κάθε executor θα έχει 4GB μνήμης
    .getOrCreate()

spark = SparkSession.builder \
    .appName("Query4_EuclideanDistance") \
    .config("spark.executor.instances", "8") \  # Χρησιμοποιούμε 8 executors
    .config("spark.executor.cores", "1") \  # Κάθε executor θα έχει 1 core
    .config("spark.executor.memory", "2g") \  # Κάθε executor θα έχει 2GB μνήμης
    .getOrCreate()

spark = SparkSession.builder \
    .appName("Query4_EuclideanDistance") \
    .config("spark.executor.instances", "2") \  # 2 executors
    .config("spark.executor.cores", "1") \  # Κάθε executor έχει 1 core
    .config("spark.executor.memory", "2g") \  # Κάθε executor έχει 2GB μνήμη
    .getOrCreate()
spark = SparkSession.builder \
    .appName("Query4_EuclideanDistance") \
    .config("spark.executor.instances", "2") \  # 2 executors
    .config("spark.executor.cores", "2") \  # Κάθε executor έχει 2 cores
    .config("spark.executor.memory", "4g") \  # Κάθε executor έχει 4GB μνήμη
    .getOrCreate()

spark = SparkSession.builder \
    .appName("Query4_EuclideanDistance") \
    .config("spark.executor.instances", "2") \  # 2 executors
    .config("spark.executor.cores", "4") \  # Κάθε executor έχει 4 cores
    .config("spark.executor.memory", "8g") \  # Κάθε executor έχει 8GB μνήμη
    .getOrCreate()

