import os
import psycopg2
from psycopg2.extras import RealDictCursor


class DB:

    def __init__(self, name):
        self.conn = psycopg2.connect(os.getenv(name))

    def read_configuration(self):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM configuration")
            return cur.fetchall()

    def read_table(self, table, columns, last_id=None):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT {','.join(columns)} FROM {table} WHERE id > %s", [last_id])
            return cur.fetchall()

    def store_results(self, table, columns, results):
        with self.conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {table} ({','.join(columns)}) VALUES %s", [results])
            self.conn.commit()

    def update_configuration(self, source_db, source_table_name, last_id):
        with self.conn.cursor() as cur:
            cur.execute(f"UPDATE configuration SET last_id = %s WHERE (source_db, source_table_name) = (%s, %s)",
                        [source_db, source_table_name, last_id])
            self.conn.commit()
