from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, when

username = "username"  #georgiannarenta,ioannisanagnostaras

spark = SparkSession.builder.appName("Query2_SparkSQL").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query_2_sql_final_output_{job_id}"


crime_data_10_19 = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2010_2019.parquet"
).select("Date Rptd", "AREA NAME", "Status Desc")

crime_data_20_25 = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2020_2025.parquet"
).select("Date Rptd", "AREA NAME", "Status Desc")

crime_data = crime_data_10_19.unionByName(crime_data_20_25)

crime_data = crime_data.filter(
    col("Date Rptd").isNotNull() &
    col("AREA NAME").isNotNull() &
    col("Status Desc").isNotNull()
)

crime_data = crime_data.withColumn("year", year(to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a")))

crime_data = crime_data.withColumn(
    "is_closed",
    when(col("Status Desc").isin("UNK", "Invest Cont"), 0).otherwise(1)
)

crime_data.createOrReplaceTempView("crime_data")

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

rows = result.collect()
for row in rows:
    print(f"Year: {row['year']}, Precinct: {row['precinct']}, Closed Case Rate: {row['closed_case_rate']}, Rank: {row['#']}")
    
result.write.mode("overwrite").parquet(output_dir)
