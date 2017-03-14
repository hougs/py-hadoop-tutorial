from argparse import ArgumentParser
import ibis
import os
import pandas as pd

ibis.options.sql.default_limit = None

# # Moving data to HDFS
#
# HDFS provides a CLI for common operations. We would like to move our data
# without leaving python. We have two options to do this:
#
# * Use the subprocess package to shellout commands to the HDFS CLI, and run
# from a machine where that CLI is installed.
# * Use Ibis' HDFS client (a wrapper around WebHDFS) to move the data.
#
# Using subprocess always feels like a hack, so let's go with the more
# straightforward HDFS connection:
#
# ## Read Data from an HDFS directory
#
# We can define the schema of the files we want to read in and create a
# temporary table for Ibis to read from.

FILE_SCHEMA = ibis.schema([('project_name', 'string'),
                           ('page_name', 'string'),
                           ('n_views', 'int64'),
                           ('n_bytes', 'int64')])

# System independent way to join paths
LOCAL_DATA_PATH = os.path.join(os.getcwd(), "pageviews-gz")
LOCAL_FILES = os.listdir(LOCAL_DATA_PATH)



def mv_files(filename, hdfs_dir, hdfs_conn):
    dir_name = hdfs_dir + filename[:-3]
    hdfs_conn.mkdir(dir_name)
    filepathtarget = '/'.join([dir_name, filename])
    hdfs_conn.put(filepathtarget, os.path.join(LOCAL_DATA_PATH, filename))
    return dir_name


# ## Create New Columns
#
# We can create new named columns using the 'mutate' method. Here, year,
# month, day, and hour are string values because we want the 0 prepended.
def extract_datetime(filename):
    _, date_str, time_str = filename.split("-")
    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[-2:]
    hour = time_str[:2]
    return year, month, day, hour


def to_pd_dt(filename):
    return pd.to_datetime(filename, format='pageviews-%Y%m%d-%H0000')

# ## Hive Metastore
# Ibis allows us to interogate the hive metastore. We can determine if
# databases or tables exists by using functions defined directly on the ibis_
# connection.
#
# It is useful for us to determine if a database exists and then create it if
#  it does not.
def safe_get_db(ibis_conn, db_name):
    if not ibis_conn.exists_database(db_name):
         ibis_conn.create_database(db_name)
    return ibis_conn.database(db_name)

# ## Insert Data
# We can then create a table from an ibis expression or insert more data into
# a table with the same schema.

def gz_2_data_insert(data_dir, ibis_conn, db_name):
    tmp_table = ibis_conn.delimited_file(hdfs_dir=data_dir,
                                  schema=FILE_SCHEMA,
                                  delimiter=' ')
    year, month, day, hour = extract_datetime(data_dir.split("/")[-1])

    tmp_w_time = tmp_table.mutate(year=year, month=month, day=day, hour=hour)

    working_db = safe_get_db(ibis_conn, db_name)
    if 'wiki_pageviews' in working_db.tables:
        ibis_conn.insert('wiki_pageviews', tmp_w_time, database=db_name)
    else:
        ibis_conn.create_table('wiki_pageviews', obj=tmp_w_time,
                               database=db_name)

# Then, wrap this all up in a function so we can use it in a list comprehension.


def main(hdfs_conn, ibis_conn, hdfs_dir, db_name):
    hdfs_gz_dirs = [mv_files(filename, hdfs_dir, hdfs_conn) for filename in
                    LOCAL_FILES]
    [gz_2_data_insert(data_dir, ibis_conn, db_name) for data_dir in hdfs_gz_dirs]


if __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument("--db_name",
                            default='u_juliet')
    arg_parser.add_argument("--hdfs_dir",
                            default='/user/juliet/pageviews-gz/')
    arg_parser.add_argument("--nn_host",
                            default='')
    arg_parser.add_argument("--impala_host",
                            default='')
    args = arg_parser.parse_args()

    hdfs_conn = ibis.hdfs_connect(host=args.nn_host)
    ibis_conn = ibis.impala.connect(host=args.impala_host,
                                port=21050,
                                hdfs_client=hdfs_conn)

    main(hdfs_conn, ibis_conn, args.hdfs_dir, args.db_name)
