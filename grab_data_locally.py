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
    main('2016', '01', '01')
