import psycopg2
from psycopg2 import pool
import os
try:
    import dotenv
except ImportError:
    print("Le package dotenv n'est pas installé.")
else:
    dotenv.load_dotenv()

class DatabasePool:
    def __init__(self):
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10, 
            dbname=os.environ['DBNAME'],
            user=os.environ['DBUSER'],
            password=os.environ['DBPWD'],
            host=os.environ['DBHOST'],
            port=os.environ['DBPORT']
        )

    def get_connection(self):
        return self.connection_pool.getconn()

    def put_connection(self, conn):
        self.connection_pool.putconn(conn)

    def close_pool(self):
        self.connection_pool.closeall()
