import os
import requests
from bs4 import BeautifulSoup
# from multiprocessing import Pool
from db import get_counties

from pprint import pprint

SCRAPER_API_KEY = os.environ.get('SCRAPER_API_KEY', '')
SCRAPERAPI_URL = 'http://api.scraperapi.com'


def get_num_of_results(first_page_soup):
    resultscount_list = first_page_soup.find(
        'span', {'class': 'resultscount'}).text.split('\xa0')
    return int(resultscount_list[5])


def main():
    counties = get_counties()
    for county in counties[:2]:
        resp = requests.get(
            SCRAPERAPI_URL,
            {'api_key': SCRAPER_API_KEY, 'url': county['landwatchurl']}
        )
        soup = BeautifulSoup(resp.content, 'html.parser')
        num_of_results = get_num_of_results(soup)
        pprint(num_of_results)


if __name__ == '__main__':
    main()
