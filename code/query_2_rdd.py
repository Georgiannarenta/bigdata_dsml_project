from pyspark.sql import SparkSession
from datetime import datetime

username = "username" #georgiannarenta,ioannisanagnostaras
spark = SparkSession.builder.appName("Query2_RDD").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query2_rdd_output_{job_id}"

crime_data_10_19 = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2010_2019.parquet"
)
crime_data_20_25 = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2020_2025.parquet"
)

crime_data_rdd = crime_data_10_19.select("Date Rptd", "AREA NAME", "Status Desc").rdd.map(
    lambda x: (x['Date Rptd'], x['AREA NAME'], x['Status Desc'])
).union(
    crime_data_20_25.select("Date Rptd", "AREA NAME", "Status Desc").rdd.map(
        lambda x: (x['Date Rptd'], x['AREA NAME'], x['Status Desc'])
    )
)

filtered = crime_data_rdd.filter(lambda x: x[0] and x[1] and x[2]).map(lambda x: (
    (datetime.strptime(x[0], "%m/%d/%Y %I:%M:%S %p").year, x[1]),
    (1, 0 if x[2] in ["UNK", "Invest Cont"] else 1)
))


counts = filtered.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))


closed_case_rate = counts.map(lambda x: (
    x[0][0],  # year
    (x[0][1], round((x[1][1] / x[1][0] * 100), 6) if x[1][0] > 0 else 0.0)
))

rank = closed_case_rate.groupByKey().flatMap(lambda x: [
    (x[0], name, rate, i + 1)
    for i, (name, rate) in enumerate(sorted(x[1], key=lambda y: -y[1])[:3])
])


sorted = rank.sortBy(lambda x: (x[0], x[3]))
for row in sorted.collect():
    print(f"{row[0]} {row[1]} {row[2]:.6f} {row[3]}")

sorted.map(lambda x: f"{x[0]}\t{x[1]}\t{x[2]:.6f}\t{x[3]}").saveAsTextFile(output_dir)
