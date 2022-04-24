import os
import psycopg2
from psycopg2.extras import RealDictCursor
import sqlalchemy as sa
from sqlalchemy import create_engine
from models import Base, User
from sqlalchemy.orm import sessionmaker


class DB:
    def __init__(self, name):
        self.conn = psycopg2.connect(os.getenv(name))
        self.engine = create_engine(os.getenv("SQLALCHEMY_ANALYTICAL_DB"))
        Session = sessionmaker(self.engine)
        self.session = Session()

    def read_configuration(self):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM configuration")
            return cur.fetchall()

    def read_table(self, table, columns, last_id=None):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT {','.join(columns)} FROM {table} WHERE id > %s order by id asc", [last_id])
            return cur.fetchall()

    def store_results(self, table, columns, results):
        model = self._get_class_by_tablename(table)

        param = dict(results)
        instance = model(**param)
        self.session.add(instance)
        # with self.conn.cursor() as cur:
        #     cur.execute(
        #         f"INSERT INTO {table} ({','.join(columns)}) VALUES %s", [results])
        #     self.conn.commit()

    def update_configuration(self, source_db, source_table_name, last_id):
        with self.conn.cursor() as cur:
            cur.execute(f"UPDATE configuration SET last_id = %s WHERE source_db = %s and source_table_name = %s",
                        [last_id, source_db, source_table_name])
            self.conn.commit()

    def _get_class_by_tablename(self, tablename):
        """Return class reference mapped to table.

        :param tablename: String with name of table.
        :return: Class reference or None.
        """
        for c in Base.registry._class_registry.values():
            if hasattr(c, '__tablename__') and c.__tablename__ == tablename:
                return c

    def create_table(self, table_name):
        # If table don't exist, Create.

        if not sa.inspect(self.engine).has_table(table_name):
            model = self._get_class_by_tablename(table_name)
            model.__table__.create(self.engine)
