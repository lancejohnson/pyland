import grequests
import datetime


def main():
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
    rs = (grequests.get(url) for url in paginated_urls)
    rs2 = grequests.map(rs)

    print(datetime.datetime.now())
    print(rs2)


if __name__ == '__main__':
    main()
