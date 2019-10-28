import os
import re
import argparse
import sys
import requests
from bs4 import BeautifulSoup
from db import get_counties
import math
import aiohttp
import asyncio
from datetime import datetime

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


def listing_parser(listing_soup, county):
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
        'date_first_seen': 'Oct 26, 2019',
        'price_per_acre': 17500.00  # this field is calculated
    }

    listing_dict = {}
    base_url = 'https://www.landwatch.com'
    listing_dict['url'] = base_url + \
        listing_soup.find('div', {'class': 'propName'}).find('a')['href']
    listing_dict['pid'] = int(listing_dict['url'][-8:])

    acres = float(listing_soup.find(text=re.compile(r'Acre')).split('Acre')[0])
    listing_dict['acres'] = acres if acres else 1
    price = int(listing_soup.find('div', {'class': 'propName'}).text.split(
        '$')[-1].strip().replace(',', ''))
    listing_dict['price'] = price if price else 1
    listing_dict['price_per_acre'] = price/acres

    title_string = listing_soup.find(
        'div', {'class': 'propName'}).text.split('$')[0].strip()
    city = re.findall(r',?[a-zA-Z][a-zA-Z0-9]*,', title_string)
    listing_dict['city'] = city[0].replace(
        ',', '') if len(city) == 2 else 'NotPresent'

    description = listing_soup.find(
        'div', {'class': 'description'}).text.strip()
    listing_dict['description'] = description if description else 'NotPresen'

    listing_dict['county'] = county['county']
    listing_dict['state'] = county['stateabbr']

    office_name = listing_soup.find('a', {'class': 'officename'}).text
    listing_dict['officename'] = office_name if office_name else 'NotPresent'
    office_url = base_url + \
        listing_soup.find('a', {'class': 'officename'})['href']
    listing_dict['officeurl'] = office_url if office_url else 'NotPresent'
    office_status = listing_soup.find(
        'div', {'class': 'propertyAgent'}).text.strip().split('\n')[1].strip()
    listing_dict['officestatus'] = office_status if office_status else 'Blank'

    listing_dict['date_first_seen'] = datetime.now().date()


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
                listing_parser(listing_soup, county)


if __name__ == '__main__':
    main()
