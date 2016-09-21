# py-hadoop-tutorial
Source Material for using Python and Hadoop together.

## Data
We will use hourly Wikipedia page view statistics that have been corrected:
[link](https://dumps.wikimedia.org/other/pageviews/2016/)

To download the data, run 

    mkdir pageviews-gz
    python grab_data.py

from the root of this directory.

## Setup a local cluster
This tutorial is designed and tested to work against CDH 5.8 and
a local cluster managed using docker and [Cloudera's clusterdock.](
https://hub.docker.com/r/cloudera/clusterdock/). All services will run
on localhost, and will have port numbers assigned during cluster startup.

