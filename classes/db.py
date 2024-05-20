import sqlite3


class Clodwalk_db:
    def __init__(self):
        self.conn = sqlite3.connect('cloudwwalk.db')
        self.cursor = self.conn.cursor()

    def create_table(self, query):
        """""create the table if not exist in the db"""""
        self.cursor.execute(query)
        self.conn.commit()

    def insert_data(self, table_name, data):
        """""Insert data into the table of db """""
        column_names = list(data[0].keys())
        insert_query = f'INSERT INTO {table_name} ({", ".join(column_names)}) VALUES (:{", :".join(column_names)})'
        self.cursor.executemany(insert_query, data)
        self.conn.commit()

    def run_query(self, query):
        """""Execute query in db"""""
        # self.cursor.execute(query)
        # result = self.cursor.fetchall()
        return self.cursor.execute(query)

    def close_connection(self):
        """""closse the connection with db"""""
        self.conn.close()
