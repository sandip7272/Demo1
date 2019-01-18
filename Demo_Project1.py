from pyspark.sql import SparkSession
import configparser

config = configparser.RawConfigParser()
config.read("config.propertise")

spark = SparkSession.builder.appName(config.get("dev", "appName")).master(config.get("dev", "executionMode")).getOrCreate()

csvDF = spark.read.format("csv").option("header","true").option("inferSchema","true").\
    load(config.get("prod","sourceFile"))

countRecordDF = csvDF.count()

print (countRecordDF)

