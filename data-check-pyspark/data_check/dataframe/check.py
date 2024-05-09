from pyspark.sql import SparkSession

print("Haha")
session = (
      SparkSession
        .builder
        .master("local[*]")
        .appName("spark test")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        # .config("spark.local.dir", "./tmp")
        #.config("spark.log.level", "WARN")
        .getOrCreate()
)
print("Buu")

# JAVA_HOME='C:\Program Files\Java\jdk-17\bin'
# PATH = %PATH%;C:\Program Files\Java\jdk1.8.0_201\bin