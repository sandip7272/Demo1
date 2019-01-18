#!/usr/bin/env python
# coding: utf-8

jsondf = spark.read.json("/home/sandip/Downloads/dataJan-17-2019.json")


jsondf.printSchema()

jsondf.show()





