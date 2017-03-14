
# coding: utf-8

# # Ibis
# 
# Note that we import that package `ibis` but when pip or conda installing we use `ibis-framework`
# 
# Turn on inline matplotlib in this notebook.

# In[ ]:

import ibis
import pandas as pd


# ## Default Row Limits
# It is possible to turn on interactive mode, which automatically executs ibis expressions. By default, ibis limits result sets returned to the local process to 10,000 rows. If you know you require >10000 rows returned, be careful to change the default limit.
# 
# ## Interactive Mode
# Ibis also allows and interactive mode that automatically executes all expressions. This can be useful in a notebook or repl. I personally prefer to epxlicitly execute expresssions, but this is a personal preference.  If you use the interactive mode, I recommnd setting the defaultlimit low to prevent accidentally trying to return an unreasonable number of rows to your local process. To safely turn on interactive mode, you would run somehting like the two commands:
# 
# ibis.options.sql.default_limit = 10
# ibis.options.interactive = True TODO double check this is correct

ibis.options.sql.default_limit = None

hdfs_conn = ibis.hdfs_connect(host='')
ibis_conn = ibis.impala.connect(host='',
                                    hdfs_client=hdfs_conn)


pageviews_tbl = ibis_conn.table('wiki_pageviews', database='u_juliet')


# What is in a project name? What does this data look like?

# In[ ]:

project_names_expr = pageviews_tbl.project_name.distinct()
project_names = ibis_conn.execute(project_names_expr)
project_names


# From the data docs, we know that the post fixes have the following meanings:
# 
#     wikibooks: ".b"
#     wiktionary: ".d"
#     wikimedia: ".m"
#     wikipedia mobile: ".mw"
#     wikinews: ".n"
#     wikiquote: ".q"
#     wikisource: ".s"
#     wikiversity: ".v"
#     mediawiki: ".w"
# 
# Maybe we can understand this by finding the projects with the most pages. Let's group by porject name and then count the size of the groups.


project_page_counts = pageviews_tbl.group_by(pageviews_tbl.project_name)                                   .size()                                   .sort_by(('count', False))
project_page_counts = ibis_conn.execute(project_page_counts)
project_page_counts


# To find something interesting, it'll help to understand the language. 

# In[ ]:

[name for name in project_names if 'en' in name]


# The part of the project name after the '.' specifies a special type of wiki. Let's just look at the standard wiki pages (ie, not media-wiki) that are also written in English.

# In[ ]:

ibis_conn.execute(pageviews_tbl[pageviews_tbl.project_name == 'en'].limit(10))


# Project_name is homogenous in this dataset, so lets just take the projection of all other columns.

# In[ ]:

en_pageviews = pageviews_tbl[pageviews_tbl.project_name == 'en'].projection(['page_name',
                                                                              'n_views',
                                                                             'n_bytes',
                                                                             'day',
                                                                             'hour',
                                                                             'month',
                                                                             'year'])


# In[ ]:

ibis_conn.execute(en_pageviews.limit(10))


# It seems that we should exclude these pages with no names, and NaN counts. (With big data sets, you will find all
# types of messed up data.)

# In[ ]:

top_10_pg_views_hourly = en_pageviews.sort_by((en_pageviews.n_views, False)).limit(10)
ibis_conn.execute(top_10_pg_views_hourly)


# In[ ]:

null_pg_views = en_pageviews[en_pageviews.n_views.isnull()]


# In[ ]:

ibis_conn.execute(null_pg_views)


# In[ ]:

nn_pg_views = en_pageviews[en_pageviews.n_views.notnull()]


# What are the top ten page in this series that 

# In[ ]:

ibis_conn.execute(nn_pg_views.sort_by((nn_pg_views.n_views, False)).limit(10))


# hangover, brands of champagne, mew years traditions, time differences, international datetime,

# In[ ]:

champagne_df = ibis_conn.execute(nn_pg_views[nn_pg_views.page_name.lower() == 'champagne'])


# In[ ]:

champagne_df.sort(['day', 'hour'])


# In[ ]:

champagne_df['time'] = pd.to_datetime(champagne_df[['year', 'month', 'day', 'hour']])


# In[ ]:

champagne_df[['n_views', 'time']].plot()


# In[ ]:

w_daily_views = nn_pg_views.group_by(['page_name', 'month', 'day']).aggregate(daily_views=nn_pg_views.n_views.sum())

ibis_conn.execute(w_daily_views.sort_by((w_daily_views.daily_views, False)).limit(10))


# In[ ]:

tot_view = nn_pg_views.group_by('page_name').aggregate(all_views=nn_pg_views.n_views.sum())
ibis_conn.execute(tot_view.sort_by((tot_view.all_views, False)).limit(30))


# ## Go Explore
# [Go forth and explore, though you may find the Ibis docs for SQL programmers helpful.](http://docs.ibis-project.org/sql.html)

# In[ ]:



