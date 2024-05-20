from classes.db import Clodwalk_db
from classes.api import Call_api
from datetime import datetime
import csv

db = Clodwalk_db()
data = Call_api()


def open_query_file(path):
    """""function to open query local file"""""
    f1 = open(path, 'r')
    return f1.read()


def creact_csv_file(path, cursor):
    """"function to create csv file"""""
    with open(path, "w") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter="\t")
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)


def create_db():
    """""function that call the create tables in SQLite"""""
    country_query = open_query_file('files/create_country_table_query.txt')
    gdp_query = open_query_file('files/create_gdp_table_query.txt')
    # result = open_query_file('files/create_result_table_query.txt')
    db.create_table(country_query)
    db.create_table(gdp_query)
    # db.create_table(result)


def create_tables(content, country_table, gdp_table):
    """""function create the dicts of table using result of api"""""
    for item in content[1]:
        country_value = {
            'id': item['country']['id'],
            'name': item['country']['value'],
            'iso3_code': item['countryiso3code'],
            'create_date': datetime.today().strftime('%Y-%m-%d')
        }
        if country_value not in country_table:
            country_table.append(country_value)

        gdp_value = {
            'country_id': item['country']['id'],
            'year': item['date'],
            'value': item['value'],
            'create_date': datetime.today().strftime('%Y-%m-%d')
        }

        gdp_table.append(gdp_value)

    return country_table, gdp_table


def main():
    create_db()
    country_table = []
    gdp_table = []

    # Get first page, for get the number of pages and data.
    first_page = data.get_data(1)

    country_table, gdp_table = create_tables(first_page, country_table, gdp_table)

    max_page = data.get_max_page(first_page)

    for page in range(2, max_page + 1):
        data.get_data(page)
        country_table, gdp_table = create_tables(first_page, country_table, gdp_table)

    db.insert_data('country', country_table)
    db.insert_data('gdp', gdp_table)

    pivot_gdp_query = open_query_file('files/pivot_gdp_query.txt')

    result_pivot_gdp_query = db.run_query(pivot_gdp_query)

    create_pivot_query = []
    for row in result_pivot_gdp_query:
        create_pivot_query.append(row[0])

    get_main_query = open_query_file('files/main_query.txt')

    main_query = get_main_query.replace('#########', ("".join(create_pivot_query)))

    result_main_query = db.run_query(main_query)

    creact_csv_file('files/result.csv', result_main_query)

    db.run_query(f"CREATE TABLE IF NOT EXISTS result AS {main_query}")

    db.close_connection()


if __name__ == '__main__':
    main()
