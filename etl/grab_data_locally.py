from argparse import ArgumentParser
import requests
from bs4 import BeautifulSoup

def get_pageviews(year, month, day):
    pageviews_url = 'https://dumps.wikimedia.org/other/pageviews/{0}/{0}-{1}/'.format(year, month)
    soup = BeautifulSoup(requests.get(pageviews_url).text)
    for a in soup.find_all('a'):
        if 'pageviews-{0}{1}{2}'.format(year, month, day) in a['href']:
            yield pageviews_url + a['href']

def write_file(url):
    req = requests.get(url, stream=True)
    local_filename = url.split("/")[-1]
    with open('pageviews-gz/' + local_filename, 'wb') as f:
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()

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
