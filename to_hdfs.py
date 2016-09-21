import ibis
import pandas as pd


ibis.options.sql.default_limit = None

hdfs_conn = ibis.hdfs_connect(host='bottou03.sjc.cloudera.com')

ibis_conn = ibis.impala.connect(host='bottou01.sjc.cloudera.com',
                                    port=21050,
                                    hdfs_client=hdfs_conn)

def mv_files(filename):
    dir_name = '/user/juliet/pageviews-gz/{}'.format(filename[:-3])
    hdfs_conn.mkdir(dir_name)
    filepathtarget='/'.join([dir_name, filename])
    hdfs_conn.put(filepathtarget, os.path.join(local_data_path, filename))
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
                                  schema=file_schema,
                                  delimiter=' ')
    pdtime = to_pd_dt(data_dir.split("/")[-1])
    # create a column named time
    tmp_w_time = tmp_table.mutate(time=pdtime)
    if 'wiki_pageviews' in db_expr.tables:
        ibis_conn.insert('wiki_pageviews', tmp_w_time, database='u_juliet')
    else:
        ibis_conn.create_table('wiki_pageviews', obj=tmp_w_time, database='u_juliet')