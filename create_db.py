import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from pymysql import connect
import pandas as pd

# Step 1: Create a DataFrame with the data

# read departments

df = pd.read_csv("uploads/departments.csv", delimiter=",",header=None)
df.columns = ["id","department"]
print("read departments succesful")


# read jobs
df_jobs = pd.read_csv("uploads/jobs.csv", delimiter=",",header=None)
df_jobs.columns = ["id","position"]
print("read jobs successful")



# Step 2: Create a SQLAlchemy engine to connect to the MySQL database
engine = create_engine("mysql+mysqlconnector://root:1234@localhost/globant".
                       format(user="root",
                               pw="1235",
                               db="globant"))


# Step 3: Convert the Pandas DataFrame to a format for MySQL table insertion

#insertion departments
df.to_sql('departments', con=engine, if_exists='append', index=False)
print("insert deparments sucessful")

# insertion jobs
df_jobs.to_sql('jobs', con=engine, if_exists='append', index=False)
print("insert jobs sucessful")

