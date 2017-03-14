from argparse import ArgumentParser
import requests
from bs4 import BeautifulSoup
# # Getting Wiki Pageview Date
# Wikipedia makes a lot of their data publicly available, inline with their
# open access philosophy. It is one of my favorite example of large (note:
# their data is particularly large because much of it is released in formats
# that do not optimize for efficient representation or reducing data
# redundancy.), publicly available datasets, because it is an incredibly
# popular website that covers a ton of domains and languages.
#
# Their data is popular and people make many interesting things with it. In
# particular, I think [Wikitrends is a great example.](http://www.wikipediatrends.com/)
# Currently on their front page they are displaying an interactive plot of
# pageviews for presidential candidate's pages.

# ![trending on wikitrends](img/trending-on-wikitrends.png)
#
# They also provide a page-customizable view for anyone to look up view numbers
#  for any page.
#
# ![lemmy views](img/search-interface.png)
#
# This data is interesting because it tells us what people are looking up on
# wikipedia. From this we can infer that their mind is on the topic. Talk about
#  a look into cultural Zeitgeist!
#
# We will use pageveiw data where the wikimedia team has already removed as
# many identifiable bot requests as possible. Documentation on this data is
# avaiable here [available here.](https://dumps.wikimedia.org/other/pagecounts-raw/)
# The data is made available in hourly files and the urls are first split by
#  year, then by month.
#
# ### dumps.wikimedia.org/other/pageviews/
# ![screen shot of file index1](img/pgvw1.png)
#
# ### dumps.wikimedia.org/other/pageviews/2016/
# ![screen shot of file index1](img/pgvw2.png)
#
# ### dumps.wikimedia.org/other/pageviews/2016/2016-01/
# ![screen shot of file index1](img/pgvw3.png)
#
# [Files reside here](https://dumps.wikimedia.org/other/pageviews/)
#
# [Docs on pageview stats](https://en.wikipedia.org/wiki/Wikipedia:Pageview_statistics)
#
# [Other, related data](https://dumps.wikimedia.org/other/analytics/)
#
# Our strategy for moving this data into HDFS is to download it locally and the then use the hdfs client 'put' command to move the data from local to HDFS. To download the data we will:
#
# * Send and HTTP request to get a page listing all files in a certain month.
# * Parse the returned HTML to retrieve all file names.
# * Filter file names by day so we don't spend our entire lives downloading data
#     (ie. only download a days worth of data at a time.)

def get_pageviews(year, month, day):
    pageviews_url = 'https://dumps.wikimedia.org/other/pageviews/{0}/{0}-{1}/'.format(year, month)
    soup = BeautifulSoup(requests.get(pageviews_url).text)
    for a in soup.find_all('a'):
        if 'pageviews-{0}{1}{2}'.format(year, month, day) in a['href']:
            yield pageviews_url + a['href']

# Given the fully qualified url we will use the requests package to retrieve
# the contents of the file hosted there. Pretty straightforward.

def write_file(url):
    req = requests.get(url, stream=True)
    local_filename = url.split("/")[-1]
    with open('pageviews-gz/' + local_filename, 'wb') as f:
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()

# For all hourly files matching the pattern we expect for January 1st 2016,
# download it and write the first one to local disk:
def main(year, month, day):
    [write_file(url) for url in get_pageviews(year, month, day)]

if __name__ == "__main__":
    arg_parser = ArgumentParser()
    # Default to January 1st, 2016
    arg_parser.add_argument("--year",
                            default='2016',
                            choices=['2016', '2015'],
                            action='store')
    arg_parser.add_argument("--month",
                            default='01',
                            choices=[str(n).zfill(2) for n in range(1, 13)],
                            action='store')
    arg_parser.add_argument("--day",
                            default='01',
                            choices=[str(n).zfill(2) for n in range(1, 32)],
                            action='store')
    args = arg_parser.parse_args()
    main(args.year, args.month, args.day)
