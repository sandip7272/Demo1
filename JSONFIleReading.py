#!/usr/bin/env python
# coding: utf-8
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Demo1").getOrCreate()
spark

jsondf = spark.read.json("/home/sandip/Downloads/dataJan-17-2019.json")


jsondf.printSchema()

jsondf.show()





