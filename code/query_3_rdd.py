from pyspark.sql import SparkSession

username = "username" #georgiannarenta,ioannisanagnostaras

spark = SparkSession.builder.appName("Query3_RDD").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query3_rdd_output_{job_id}"

pop_file = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_population_by_zip_2010.parquet"
).rdd

income_file = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_income_by_zip_2015.parquet"
).rdd


pop_rdd = pop_file.map(lambda x: (x["Zip Code"], x["Average Household Size"]))
income_rdd = income_file.map(lambda x: (x["Zip Code"], x["Estimated Median Income"]))

joined_rdd = pop_rdd.join(income_rdd)

result_rdd = joined_rdd.map(lambda x: (
    x[0],
    round(x[1][1] / x[1][0], 6)))

for item in result_rdd.collect():
    print(item)
  
result_rdd.saveAsTextFile(output_dir)
