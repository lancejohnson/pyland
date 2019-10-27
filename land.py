import os
import argparse
import sys
import requests
from bs4 import BeautifulSoup
from db import get_counties
import math
import aiohttp
import asyncio

SCRAPER_API_KEY = os.environ.get('SCRAPER_API_KEY', '')
SCRAPERAPI_URL = 'http://api.scraperapi.com'


def get_num_of_results(first_page_soup):
    resultscount_list = first_page_soup.find(
        'span', {'class': 'resultscount'}).text.split('\xa0')
    # Quest: why does this kind of soup yield characters like
    # '\xa0' when I use soup.text?
    return int(resultscount_list[5])


def gen_paginated_urls(first_page_soup, num_of_results, CON_LIMIT):
    paginated_urls = []
    if num_of_results > 15:
        num_of_pages = math.ceil(num_of_results / 15)
        pagination_base_url = first_page_soup.find(
            'a', {'rel': 'next'})['href'][:-1]
        for i in range(2, num_of_pages + 1):
            paginated_urls.append(f'{pagination_base_url}{i}')
        paginated_urls = [paginated_urls[i:i+CON_LIMIT]
                          for i in range(0, len(paginated_urls), CON_LIMIT)]
    return paginated_urls


async def fetch(session, url):
    SCRAPER_API_KEY = os.environ.get('SCRAPER_API_KEY', '')
    SCRAPERAPI_URL = 'http://api.scraperapi.com'
    try:
        async with session.get(
                SCRAPERAPI_URL,
                params={'api_key': SCRAPER_API_KEY, 'url': url}) as response:
            return await response.text()
            # print(url, datetime.datetime.now(), 'END')
            # return res
    except Exception as e:
        print(url, e)
        return None


async def get_serps_response(paginated_urls):
    async with aiohttp.ClientSession() as session:
        soups = []
        for serp_url_block in paginated_urls:
            responses = await asyncio.gather(
                *[fetch(session, url) for url in serp_url_block])
            soups.extend(
                [BeautifulSoup(resp, 'html.parser') for resp in responses])
        return soups


def listing_parser(listing_soup):
    '''This takes the soup for an individual property listing and transforms
    it into the following schema
     '''
    example_dict = {  # noqa: F841
        'pid': 25009439,
        'listing_url': 'https://www.landwatch.com/Coconino-County-Arizona-Land-for-sale/pid/25009439',  # noqa: E501
        'city': 'Flagstaff',
        'state': 'AZ',
        'price': 2800000,
        'acres': 160.00,
        'description': 'JUST REDUCED $310,000! Absolutely beautiful 160 acre parcel completely surrounded by the Coconino National Forest within 2 miles of Flagstaff city limits. ... ',  # noqa: E501
        'office_name': 'First United Realty, Inc.',
        'office_url': 'https://www.landwatch.com/default.aspx?ct=r&type=146,157956',  # noqa: E501
        'office_status': 'Signature Partner',
        'date_first_seen': datetime(2019, 10, 26, 20, 31, 32, 651887),
        'price_per_acre': 17500.00  # this field is calculated
    }

    listing_dict = {}
    base_url = 'https://www.landwatch.com'
    listing_dict['url'] = base_url + listing_soup.find('a')['href']
    listing_dict['pid'] = int(listing_dict['url'][-7:])

    import pdb
    pdb.set_trace()


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        '-c',
        '--con_limit',
        nargs=1,
        help='Concurrency limit for requests.',
        default=10
    )

    args = p.parse_args(sys.argv[1:])
    CON_LIMIT = args.con_limit

    counties = get_counties()
    for county in counties[1:3]:
        resp = requests.get(
            SCRAPERAPI_URL,
            {'api_key': SCRAPER_API_KEY, 'url': county['landwatchurl']}
        )
        first_page_soup = BeautifulSoup(resp.content, 'html.parser')

        num_of_results = get_num_of_results(first_page_soup)
        paginated_urls = gen_paginated_urls(
            first_page_soup, num_of_results, CON_LIMIT)

        soups = [first_page_soup]
        soups.extend(asyncio.run(get_serps_response(paginated_urls)))

        for soup in soups:
            listings_soup_list = soup.select('div.result')
            for listing_soup in listings_soup_list:
                listing_parser(listing_soup)


if __name__ == '__main__':
    main()
