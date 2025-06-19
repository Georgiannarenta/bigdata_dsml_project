from pyspark.sql import SparkSession 

username = "username" #georgiannarenta #ioannisanagnostaras
spark = SparkSession.builder.appName("Query 1 RDD").getOrCreate()                        
sc.setLogLevel("ERROR") 
job_id = sc.applicationId 
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query1_rdd_output_{job_id}"   
crime_data_10_19 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2010_2019").rdd
crime_data_20_25 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/LA_Crime_Data_2020_2025").rdd
crime_data_10_19_filtered = crime_data_10_19.map(lambda x: (x['Crm Cd Desc'], x['Vict Age']))
crime_data_20_25_filtered = crime_data_20_25.map(lambda x: (x['Crm Cd Desc'], x['Vict Age']))
crime_data_rdd = crime_data_10_19_filtered.union(crime_data_20_25_filtered)

filtered = crime_data_rdd.filter(lambda x: x[0] and "AGGRAVATED ASSAULT" in x[0] and x[1])

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

assault_rdd = (
    filtered
    .map(lambda x: (age_group(x[1]), 1))
    .filter(lambda x: x[0] is not None)
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], ascending=False)
)

for item in assault_rdd.collect():
    print(item)

assault_rdd.saveAsTextFile(output_dir)
