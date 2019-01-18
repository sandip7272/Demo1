#!/usr/bin/env python
# coding: utf-8

# In[ ]:


jsondf = spark.read.json("/home/sandip/Downloads/dataJan-17-2019.json")


# In[ ]:


jsondf.printSchema()


# In[ ]:


jsondf.show()


# In[ ]:




