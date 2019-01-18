from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Demo1").getOrCreate()
spark
csvDF = spark.read.format("csv").option("header","true").option("inferSchema","true").\
    load("/usr/local/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/people.csv")
csvDF.printSchema()