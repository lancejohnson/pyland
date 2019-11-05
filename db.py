import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs
import os

PYLAND_DB_USER = os.environ.get('PYLAND_DB_USER', '')
PYLAND_DB_PWD = os.environ.get('PYLAND_DB_PWD', '')
POSTGRES_HOST = 'salt.db.elephantsql.com'
PORT = '5432'
PYLAND_DB = PYLAND_DB_USER

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
        'date_first_seen': '20191026',
        'price_per_acre': 17500.00  # this field is calculated
}


def write_listing(listing_dict):
    try:
        connection = psycopg2.connect(
            user=PYLAND_DB_USER,
            password=PYLAND_DB_PWD,
            host=POSTGRES_HOST,
            port=PORT,
            database=PYLAND_DB)
        insert = 'insert into listings (%s) values %s;'
        # Quest: I don't understand why we made this list of tuples
        # Source https://bit.ly/2JSYTjA
        tuple_list = [(key, value) for key, value in listing_dict.items()]
        columns = ','.join([tup[0] for tup in tuple_list])
        values = tuple([tup[1] for tup in tuple_list])
        cursor = connection.cursor()
        insert_query = cursor.mogrify(insert, ([AsIs(columns)] + [values])).decode('utf-8')
        cursor.execute(insert_query)
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while writing data to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def get_counties():
    try:
        connection = psycopg2.connect(
            user=PYLAND_DB_USER,
            password=PYLAND_DB_PWD,
            host=POSTGRES_HOST,
            port=PORT,
            database=PYLAND_DB)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        postgreSQL_select_Query = "select * from counties"

        cursor.execute(postgreSQL_select_Query)
        print("Selecting rows from counties table using cursor.fetchall")
        county_list = cursor.fetchall()

        county_dict = []
        for row in county_list:
            county_dict.append(dict(row))

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    return county_dict
