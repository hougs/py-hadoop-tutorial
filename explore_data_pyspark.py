# # Now again, but with PySpark
#
from pyspark.sql import SparkSession

# Enabling Hive support allows us to easily interact with the Hive Metastore
spark = SparkSession.builder \
            .master("yarn") \
            .appName("Exploring Pageviews With PySpark") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.executor.cores", "3") \
            .config("spark.executors.instances", "1") \
            .enableHiveSupport() \
            .getOrCreate()

#spark.sparkContext.setLogLevel("OFF")
pageviews_tbl = spark.sql("SELECT * FROM u_juliet.sm_sample").cache()
project_names = pageviews_tbl.select("project_name").distinct().collect()
project_names

project_names_list = [row.project_name for row in project_names]

project_page_counts = pageviews_tbl.select("project_name").groupBy("project_name").count().orderBy("count",
ascending=False).collect()
project_page_counts

[name for name in project_names_list if 'en' in name]

pageviews_tbl.filter("project_name = 'en'").show(10)


en_pageviews = pageviews_tbl.filter("project_name= 'en'").drop("project_name")
en_pageviews.show(10)

top_10_pg_views_hourly = en_pageviews.orderBy("n_views", ascending=False)
top_10_pg_views_hourly.show(10)



null_pg_views = en_pageviews.filter("n_views IS NULL")
null_pg_views.show()

nn_pg_views = en_pageviews.filter("n_views IS NOT NULL")
nn_pg_views.orderBy("n_views", ascending=False).show(10)

champagne_df = nn_pg_views.filter("LOWER(page_name) = 'champagne'")
champagne_df.orderBy("day", "hour").show(10)


w_daily_views = nn_pg_views.groupBy("page_name", "month",
"day").sum("n_views").withColumnRenamed("sum(n_views)",
"daily_views").orderBy("daily_views", ascending=False)
w_daily_views.show(10)


tot_view = nn_pg_views.groupBy("page_name").sum("n_views").withColumnRenamed("sum(n_views)",
"all_views").orderBy("all_views", ascending=False)
tot_view.show(30)

