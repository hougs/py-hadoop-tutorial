
# coding: utf-8

# # Now again, but with PySpark
# 
# ## Launching the Notebook
# 
# Generally, we need to import SparkContext and instantiate one. Because we launched this notebook with the env variable SPARK_DRIVER_PYTHON=ipython, we magically have an instantiated spark context available to us already.
# 
# The full launch scirpt for the Jupyter notebook servers you are using is:
#     #!/bin/bash
#     
#     export PYSPARK_DRIVER_PYTHON=ipython
#     export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.ip='*'
#     --NotebookApp.open_browser=False --NotebookApp.port=8880"
# 
#     export HADOOP_CONF_DIR=/etc/hive/conf
#     export HIVE_CP=/opt/cloudera/parcels/CDH/lib/hive/lib/
# 
#     pyspark --master yarn --deploy-mode client --driver-memory 2g \
#     --num-executors 1 --executor-memory 8g --executor-cores 3

# In[ ]:

from pyspark.sql import HiveContext


# Using a HiveContext allows us to easily interact with the Hive Metastore

# In[ ]:

sqlContext = HiveContext(sc)


# In[ ]:

pageviews_tbl = sqlContext.sql("SELECT * FROM u_srowen.sm_sample").cache()
project_names = pageviews_tbl.select("project_name").distinct().collect()
project_names


# In[ ]:

project_page_counts = pageviews_tbl.select("project_name").groupBy("project_name").count().orderBy("count",
ascending=False).collect()
project_page_counts


# In[ ]:

[name for name in project_names if 'en' in name]


# In[ ]:

pageviews_tbl.filter("project_name = 'en'").show(10)


# In[ ]:

en_pageviews = pageviews_tbl.filter("project_name= 'en'").drop("project_name")
en_pageviews.show(10)


# In[ ]:

top_10_pg_views_hourly = en_pageviews.orderBy("n_views", ascending=False)
top_10_pg_views_hourly.show(10)


# In[ ]:

null_pg_views = en_pageviews.filter("n_views IS NULL")
null_pg_views.show()


# In[ ]:

nn_pg_views = en_pageviews.filter("n_views IS NOT NULL")
nn_pg_views.orderBy("n_views", ascending=False).show(10)


# In[ ]:

champagne_df = nn_pg_views.filter("LOWER(page_name) = 'champagne'")
champagne_df.orderBy("day", "hour").show(10)


# In[ ]:

w_daily_views = nn_pg_views.groupBy("page_name", "month",
"day").sum("n_views").withColumnRenamed("sum(n_views)",
"daily_views").orderBy("daily_views", ascending=False)
w_daily_views.show(10)


# In[ ]:

tot_view = nn_pg_views.groupBy("page_name").sum("n_views").withColumnRenamed("sum(n_views)",
"all_views").orderBy("all_views", ascending=False)
tot_view.show(30)


# In[ ]:



