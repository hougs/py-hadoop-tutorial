import ibis
import os
import pandas as pd


ibis.options.sql.default_limit = None

hdfs_conn = ibis.hdfs_connect(host='bottou03.sjc.cloudera.com')

ibis_conn = ibis.impala.connect(host='bottou01.sjc.cloudera.com',
                                port=21050,
                                hdfs_client=hdfs_conn)

FILE_SCHEMA = ibis.schema([('project_name', 'string'),
                           ('page_name', 'string'),
                           ('monthly_total', 'int64'),
                           ('hourly_total', 'int64')])

# System independent way to join paths
LOCAL_DATA_PATH = os.path.join(os.getcwd(), "pageviews-gz")
LOCAL_FILES = os.listdir(LOCAL_DATA_PATH)


def mv_files(filename):
    dir_name = '/user/juliet/pageviews-gz/{}'.format(filename[:-3])
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


def gz_2_data_insert(data_dir, db_expr):
    tmp_table = ibis_conn.delimited_file(hdfs_dir=data_dir,
                                  schema=FILE_SCHEMA,
                                  delimiter=' ')
    year, month, day, hour = extract_datetime(data_dir.split("/")[-1])
    # create a column named time
    tmp_w_time = tmp_table.mutate(year=year, month=month, day=day, hour=hour)
    if 'wiki_pageviews' in db_expr.tables:
        ibis_conn.insert('wiki_pageviews', tmp_w_time, database='u_juliet')
    else:
        ibis_conn.create_table('wiki_pageviews', obj=tmp_w_time, database='u_juliet')


def main():
    hdfs_gz_dirs = [mv_files(filename) for filename in LOCAL_FILES]

    db_name = 'u_juliet'
    if ibis_conn.exists_database(db_name):
        user_db = ibis_conn.database(db_name)
    else:
        user_db = ibis_conn.create_database('u_juliet')

    [gz_2_data_insert(data_dir, user_db) for data_dir in hdfs_gz_dirs]



if __name__ == "__main__":
    main()
