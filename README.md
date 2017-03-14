# py-hadoop-tutorial
Source Material for using Python and Hadoop together.

## Dependencies
This is a tutorial for using Ibis and PySpark to interact with data stored in
Hadoop, particularly files in HDFS and Impala Table.

You will need access to a Hadoop cluster (or a VM/Docker image), a python
interpreter with the packages listed in requirements.txt installed, and Spark
1.6.1 [installed as described in the 'Linking with Spark' section of the docs
.](http://spark.apache.org/docs/latest/programming-guide.html)

## Data
We will use hourly Wikipedia page view statistics that have been corrected:
[link](https://dumps.wikimedia.org/other/pageviews/2016/). More documentation
on the data source can be found on the [wikitech wiki page for the dataset.]
(https://wikitech.wikimedia.org/wiki/Analytics/Data/Pageviews)

To download the data locally, run

    mkdir pageviews-gz
    python grab_data.py

from the root of this directory.

To then create the required tables,


