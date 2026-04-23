from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("exploration").getOrCreate()

csv_path = "DATA/synthetic_student_learning_dataset_10000.csv"
df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
df.createOrReplaceTempView("students")

df.printSchema()

spark.sql("select education_level from students limit 5").show()
