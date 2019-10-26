import aiohttp
import asyncio
from aiostream import stream, pipe


async def get_soups(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            print(f'{url} running')
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
    xs = stream.iterate(paginated_urls) | pipe.map(
        get_soups, task_limit=1)
    print(xs)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
