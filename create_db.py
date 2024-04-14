import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from pymysql import connect
import pandas as pd


'''
# connect to database

data_base = connect(host = 'localhost', user = 'root', passwd = '1234')

# define cursos object

cur = data_base.cursor()

# define de query

query = 'show databases'

# execute query

cur.execute(query)
'''

''''
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="1234",
  database=" globant"
) 


mycursor = mydb.cursor()
'''
#mycursor.execute("CREATE OR REPLACE TABLE departments (id INT PRIMARY KEY, department VARCHAR(255))")

# Step 1: Create a DataFrame with the data
df = pd.read_csv("uploads/departments.csv", delimiter=",",header=None)
df.columns = ["id","department"]
print(df)


# Step 2: Create a SQLAlchemy engine to connect to the MySQL database
engine = create_engine("mysql+mysqlconnector://root:1234@localhost/globant".
                       format(user="root",
                               pw="1235",
                               db="globant"))

# Step 3: Convert the Pandas DataFrame to a format for MySQL table insertion
df.to_sql('departments', con=engine, if_exists='append', index=False)



