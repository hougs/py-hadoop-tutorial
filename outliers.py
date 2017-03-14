
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

# Enabling Hive support allows us to easily interact with the Hive Metastore
spark = SparkSession.builder \
            .master("yarn") \
            .appName("Exploring Outlier Pageviews With PySpark") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.executor.cores", "3") \
            .config("spark.executors.instances", "1") \
            .enableHiveSupport() \
            .getOrCreate()

spark.sparkContext.setLogLevel("OFF")


# You all are only working with a single executor, so processing is going to go slowly. I've created a sample of the whole data set. I would recommend working with samples of the whole dataset in order to have work you launch complete in a reasonable amount of time.

hourly_pageviews_tbl = spark.sql("SELECT * FROM u_juliet.sm_sample").cache()


# We are going to transform our data into an RDD of tuples where the first value is the page name and the second values is a pd.Series.

# In[ ]:

def to_keyed_ts(row_rdd):
    """Transforms a dataframe into an rdd keyed by (page_name, (timestamp, count)) and
    an associated value that is a pandas Series of counts of pageviews indexed by timestamp.
    """
    select_data = row_rdd.rdd.map(lambda row: row_to_tuple(row))
    all_as_rdd = select_data.groupByKey()                            .mapValues(lambda iterable: to_series(iterable.data))
    return all_as_rdd.filter(lambda x: x[1] is not None)

def row_to_tuple(row):
    timestamp = pd.to_datetime("{0}-{1}-{2} {3}:00:00".format(row.year,
                                                              row.month,
                                                              row.day,
                                                              row.hour))
    return (row.page_name, (timestamp, row.n_views))

def to_series(tuples):
    """Transforms a list of tuples of the form (date, count) in to a pandas
    series indexed by dt.
    """
    cleaned_time_val_tuples = [tuple for tuple in tuples if not (
        tuple[0] is pd.NaT or tuple[1] is None)]
    if len(cleaned_time_val_tuples) > 0:
        # change list of tuples ie [(a1, b1), (a2, b2), ...] into
        # tuple of lists ie ([a1, a2, ...], [b1, b2, ...])
        unzipped_cleaned_time_values = zip(*cleaned_time_val_tuples)
        # just being explicit about what these are
        counts = unzipped_cleaned_time_values[1]
        timestamps = unzipped_cleaned_time_values[0]
        # Create the series with a sorted index.
        ret_val = pd.Series(counts, index=timestamps).sort_index()
    else:
        ret_val = None
    return ret_val


# In[ ]:

timeseries_rdd = to_keyed_ts(hourly_pageviews_tbl)


# Define a function that we can apply to a pandas series that will return the subset of the pandas series that satisfies Tukey's outlier criteria.

# In[ ]:

def flag_outliers(series, iqr_multiplier=1.5):
    """Use Tukey's boxplot criterion for outlier identification.
    """
    top_quartile_cutoff = np.percentile(series.get_values(), 75)
    bottom_quartile_cutoff = np.percentile(series.get_values(), 25)
    # Compute interquartile range
    iqr = top_quartile_cutoff - bottom_quartile_cutoff
    top_outlier_cutoff = top_quartile_cutoff + iqr * iqr_multiplier
    bottom_outlier_cutoff = bottom_quartile_cutoff - iqr * iqr_multiplier
    return series[(series < bottom_outlier_cutoff) | (series > top_outlier_cutoff)]


# In[ ]:

outlier_rdd = timeseries_rdd.mapValues(flag_outliers)


# In[ ]:

outlier_rdd.take(10)

