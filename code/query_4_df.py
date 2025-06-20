from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, lower, trim, broadcast, sqrt, pow, min as spark_min, avg, count
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder \
    .appName("Query4_EuclideanDistance") \
    .config("spark.sql.shuffle.partitions", "200") \  # Αν θες να αλλάξεις τον αριθμό των partitions
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

# Αποθήκευση αποτελεσμάτων σε φάκελο (output directory)
output_dir = "hdfs://hdfs-namenode:9000/user/username/output/Query4_results"

# Φορτώνουμε τα parquet αρχεία (αν τα έχεις ήδη μετατρέψει)
crime_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/username/data/parquet/LA_Crime_Data_2010_2019.parquet") \
            .union(spark.read.parquet("hdfs://hdfs-namenode:9000/user/username/data/parquet/LA_Crime_Data_2020_2025.parquet"))

police_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/username/data/parquet/LA_Police_Stations.parquet")

mo_codes_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/username/data/parquet/MO_codes.parquet")

# 1. Διαχωρισμός mocodes με κενό και αφαίρεση κενών
crime_exp = crime_df.withColumn("mo_code", explode(split(col("mocodes"), " "))) \
                   .filter(col("mo_code") != "")

# 2. Φιλτράρισμα Null Island (latitude=0 & longitude=0)
crime_exp = crime_exp.filter((col("latitude") != 0) & (col("longitude") != 0))

# 3. Φιλτράρουμε τα mocodes που αντιστοιχούν σε όπλα (περιγραφή περιέχει 'gun' ή 'weapon')
mo_codes_filtered = mo_codes_df.filter(
    (lower(col("description")).contains("gun")) | (lower(col("description")).contains("weapon"))
).select("code")

# Join για να κρατήσουμε μόνο εγκλήματα με όπλα
crime_weapons = crime_exp.join(mo_codes_filtered, crime_exp.mo_code == mo_codes_filtered.code, "inner")

# 4. Κανονικοποίηση ονομάτων τμημάτων (division) σε κεφαλαία για join με precinct
crime_weapons = crime_weapons.withColumn("precinct_upper", upper(col("precinct")))

# 5. Κάνουμε join ώστε να υπολογίσουμε απόσταση από κάθε έγκλημα σε κάθε τμήμα
police_df = police_df.withColumnRenamed("latitude", "station_lat") \
                     .withColumnRenamed("longitude", "station_lon") \
                     .withColumn("division_upper", upper(col("division")))

# 6. Υπολογισμός ευκλείδειας απόστασης
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

# 7. Βρίσκουμε το πλησιέστερο τμήμα ανά έγκλημα
window_spec = Window.partitionBy("crime_id").orderBy(col("distance").asc())

closest_df = cross_join_df.withColumn("rn", row_number().over(window_spec)) \
                          .filter(col("rn") == 1)

# 8. Υπολογισμός αποτελεσμάτων ανά αστυνομικό τμήμα (division_upper)
result = closest_df.groupBy("division_upper") \
    .agg(
        count("*").alias("#"),
        avg("distance").alias("average_distance")
    ) \
    .orderBy(col("#").desc())

# 9. Εμφάνιση αποτελεσμάτων και αποθήκευση
result.show(truncate=False)

# Αποθήκευση των αποτελεσμάτων στον HDFS
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

