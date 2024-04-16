from sqlalchemy import create_engine
from pymysql import Date, connect

# Create a SQLAlchemy engine to connect to the MySQL database
class Database:
    def connection(self):
        engine = create_engine("mysql+mysqlconnector://root:1234@localhost/globant".
                       format(user="root",
                               pw="1235",
                               db="globant"))
        return engine