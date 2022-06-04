from enum import Enum


class Postgres:

    def __init__(self, csv_files, ip='127.0.0.1', port=5432):
        self.ip = ip
        self.port = port
        self.csv_files = csv_files
        self.initialize()

    def initialize(self):
        for db in Databases:
            self.create_db(db.value)
        self.create_oltp_tables(Databases.OLTP.value, Tables)
        self.create_dwh_tables(Databases.DWH.value, Tables)

    def connect(self):
        pass

    def create_db(self, db_name):
        pass

    def create_oltp_tables(self, db_name, tables):
        pass

    def create_dwh_tables(self, db_name, tables):
        pass

    def create_dwh_fact_table(self):
        pass


class Tables(Enum):
    CUSTOMER = {'customer': []}
    ORDER_INFO = {'order_info': []}
    ORDER_LINE = {'orderline': []}
    PRODUCT = {'product': []}


class Databases(Enum):
    OLTP = 'oltp_db'
    DWH = 'dwh_db'
