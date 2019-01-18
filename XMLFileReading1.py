#!/usr/bin/env python
# coding: utf-8

# In[ ]:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Demo1").getOrCreate()
spark

xmldf1 = spark.read.format("com.databricks.spark.xml")        .option("rowTag","record").option("rootTag", "records")        .load("/home/sandip/Downloads/dataJan-16-2019.xml")


# In[ ]:


xmldf1.printSchema()


# In[ ]:


xmldf1.count()


# In[ ]:


xmldf1.take(4)


# In[ ]:


xmldf1.select("empid").show()


# In[ ]:


xmldf1.createOrReplaceTempView("Employee")


# In[ ]:


xmlsql = spark.sql("select empid,empname,empsal from Employee")


# In[ ]:


xmlsql.show()


# In[ ]:




