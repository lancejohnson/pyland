import os
import datetime
from requests_threads import AsyncSession

SCRAPER_API_KEY = os.environ.get('SCRAPER_API_KEY', '')
SCRAPERAPI_URL = 'http://api.scraperapi.com'

session = AsyncSession(n=100)


async def main():
    print(datetime.datetime.now())
    paginated_urls = [
        'http://httpbin.org/ip',
        'http://httpbin.org/ip',
        'http://httpbin.org/ip',
        'http://httpbin.org/ip',
        'http://httpbin.org/ip',
        'http://httpbin.org/ip',
        'http://httpbin.org/ip',
        'http://httpbin.org/ip',
        'http://httpbin.org/ip',
        'http://httpbin.org/ip',
        'http://httpbin.org/ip',
        'http://httpbin.org/ip',
    ]
    rs = []
    for url in paginated_urls:
        rs.append(await session.get(
            SCRAPERAPI_URL, params={'api_key': SCRAPER_API_KEY, 'url': url}
        ))
    print(datetime.datetime.now())
    print(rs)

if __name__ == '__main__':
    session.run(main)
