from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .appName("WeatherAnalyticsHDFS") \
        .master("local[*]") \
        .getOrCreate()

hdfs_path = "hdfs://localhost:9000/weather/data/weather_data.csv"
df = spark.read.option("header","true").option("inferSchema","true").csv(hdfs.path)
print("====== Weather Dataset Schema from HDFS ===")
df.printSchema()
print("====== Sample Weather Records === ") 
df.show(5) 
spark.stop()

