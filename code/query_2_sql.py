from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, when

username = "username"  #georgiannarenta,ioannisanagnostaras

spark = SparkSession.builder.appName("Query2_SparkSQL").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query_2_sql_output_{job_id}"

# Φορτώνουμε δεδομένα και επιλέγουμε μόνο τις απαιτούμενες στήλες
crime_data_10_19 = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2010_2019.parquet"
).select("Date Rptd", "AREA NAME", "Status Desc")

crime_data_20_25 = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2020_2025.parquet"
).select("Date Rptd", "AREA NAME", "Status Desc")

# Ενοποίηση datasets
crime_data = crime_data_10_19.unionByName(crime_data_20_25)

# Φιλτράρουμε null τιμές
crime_data = crime_data.filter(
    col("Date Rptd").isNotNull() &
    col("AREA NAME").isNotNull() &
    col("Status Desc").isNotNull()
)

# Προσθέτουμε έτος
crime_data = crime_data.withColumn("year", year(to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a")))

# Ορίζουμε στήλη is_closed (1 αν περατωμένη, 0 αν όχι)
crime_data = crime_data.withColumn(
    "is_closed",
    when(col("Status Desc").isin("UNK", "Invest Cont"), 0).otherwise(1)
)

# Δημιουργούμε προσωρινό view για Spark SQL
crime_data.createOrReplaceTempView("crime_data")

# Το SQL query χωρίς WITH
query = """
SELECT year, precinct, closed_case_rate, rank AS `#`
FROM (
    SELECT
        year,
        `AREA NAME` AS precinct,
        ROUND(COUNT(CASE WHEN is_closed = 1 THEN 1 END) * 100.0 / COUNT(*), 6) AS closed_case_rate,
        ROW_NUMBER() OVER (
            PARTITION BY year 
            ORDER BY 
                ROUND(COUNT(CASE WHEN is_closed = 1 THEN 1 END) * 100.0 / COUNT(*), 6) DESC,
                `AREA NAME`
        ) AS rank
    FROM crime_data
    GROUP BY year, `AREA NAME`
) ranked
WHERE rank <= 3
ORDER BY year, `#`
"""

result = spark.sql(query)
result.show(truncate=False)
result.write.mode("overwrite").parquet(output_dir)
