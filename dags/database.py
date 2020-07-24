from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData


class Database:

    def __init__(self):
        self.engine = create_engine('postgresql://postgres:postgres@localhost:5433/timetracko')

    def table(self, table, engine):
        return Table(
                table,
                MetaData(bind=None),
                autoload=True,
                autoload_with=engine
            )
