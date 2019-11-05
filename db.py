import psycopg2
import psycopg2.extras
import os

PYLAND_DB_USER = os.environ.get('PYLAND_DB_USER', '')
PYLAND_DB_PWD = os.environ.get('PYLAND_DB_PWD', '')


def get_counties():
    try:
        connection = psycopg2.connect(
            user=PYLAND_DB_USER,
            password=PYLAND_DB_PWD,
            host="salt.db.elephantsql.com",
            port="5432",
            database="ogekefee")
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
