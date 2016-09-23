from argparse import ArgumentParser
import ibis
import os
import pandas as pd


ibis.options.sql.default_limit = None


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


def extract_datetime(filename):
    _, date_str, time_str = filename.split("-")
    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[-2:]
    hour = time_str[:2]
    return year, month, day, hour


def to_pd_dt(filename):
    return pd.to_datetime(filename, format='pageviews-%Y%m%d-%H0000')


def gz_2_data_insert(data_dir, ibis_conn, db_name):
    tmp_table = ibis_conn.delimited_file(hdfs_dir=data_dir,
                                  schema=FILE_SCHEMA,
                                  delimiter=' ')
    year, month, day, hour = extract_datetime(data_dir.split("/")[-1])
    # create a column named time
    tmp_w_time = tmp_table.mutate(year=year, month=month, day=day, hour=hour)

    working_db = safe_get_db(ibis_conn, db_name)
    if 'wiki_pageviews' in working_db.tables:
        ibis_conn.insert('wiki_pageviews', tmp_w_time, database=db_name)
    else:
        ibis_conn.create_table('wiki_pageviews', obj=tmp_w_time,
                               database=db_name)

def safe_get_db(ibis_conn, db_name):
    if ibis_conn.exists_database(db_name):
        user_db = ibis_conn.database(db_name)
    else:
        user_db = ibis_conn.create_database(db_name)
    return user_db


def main(hdfs_conn, ibis_conn, hdfs_dir, db_name):
    #hdfs_gz_dirs = [mv_files(filename, hdfs_dir, hdfs_conn) for filename in
    #                LOCAL_FILES]
    hdfs_gz_dirs = [hdfs_dir + dir for dir in hdfs_conn.ls(hdfs_dir)]
    [gz_2_data_insert(data_dir, ibis_conn, db_name) for data_dir in hdfs_gz_dirs]


if __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument("--db_name",
                            default='u_juliet')
    arg_parser.add_argument("--hdfs_dir",
                            default='/user/juliet/pageviews-gz/')
    arg_parser.add_argument("--nn_host",
                            default='bottou03.sjc.cloudera.com')
    arg_parser.add_argument("--impala_host",
                            default='bottou01.sjc.cloudera.com')
    args = arg_parser.parse_args()

    hdfs_conn = ibis.hdfs_connect(host=args.nn_host)
    ibis_conn = ibis.impala.connect(host=args.impala_host,
                                port=21050,
                                hdfs_client=hdfs_conn)

    main(hdfs_conn, ibis_conn, args.hdfs_dir, args.db_name)
