import asyncio
import aiohttp
from aiostream import stream, pipe


async def get_soups(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            print('Sleeping')
            return await response.text()


async def main():
    paginated_urls = [
        'http://httpbin.org/ip?1',
        'http://httpbin.org/ip?2',
        'http://httpbin.org/ip?3',
        'http://httpbin.org/ip?4',
        'http://httpbin.org/ip?5',
        'http://httpbin.org/ip?6',
        'http://httpbin.org/ip?7',
        'http://httpbin.org/ip?8',
        'http://httpbin.org/ip?9',
        'http://httpbin.org/ip?10',
        'http://httpbin.org/ip?11',
        'http://httpbin.org/ip?12',
    ]
    first_page_soup = 'Soup 1'
    soups = []

    additional_soups = stream.iterate(
        paginated_urls) | pipe.map(get_soups, task_limit=2)

    soups.append(first_page_soup)
    soups.extend(additional_soups)

    print(len(soups))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
