import os
from flask import request, Blueprint, current_app
from werkzeug.utils import secure_filename
import pandas as pd
from sqlalchemy import create_engine
from database_connect import Database

UPLOAD_FOLDER = './uploads'
ALLOWED_EXTENSIONS = {'csv'}

employees = Blueprint("employees", __name__)


def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@employees.get('/employees-by-departments')
def employees_by_departments_query():
    database = Database()
    engine = database.connection()
    query = ''' WITH deparments AS
(
SELECT DISTINCT
	id,
    department
FROM globant.departments d
),

employees AS
(
SELECT DISTINCT
	id,
    id_deparment
FROM globant.employees e
),

mean AS
(
SELECT
	AVG(m.conteo) as mean
FROM(
SELECT DISTINCT
	e.id_deparment,
	COUNT(DISTINCT e.id) as conteo
FROM globant.employees e
WHERE YEAR(hired_date) = '2021'
GROUP BY 1) as m
), 

summary AS (
SELECT 
	d.id,
    d.department,
    count(distinct e.id) AS conteo
FROM employees e
INNER JOIN deparments d
	ON (e.id_deparment = d.id)
GROUP BY 1,2
    )
    
SELECT 
	s.id,
    s.department,
    s.conteo
FROM summary s, mean m
WHERE s.conteo > m.mean '''
    with engine.connect() as conn, conn.begin():  
        data = pd.read_sql(query, conn)
        print(data)  
    return data.to_json(),200

@employees.get('/jobs-by-quarter')
def jobs_by_quarter_query():
    database = Database()
    engine = database.connection()
    query = ''' WITH deparments AS
                (
                SELECT DISTINCT
                    id,
                    department
                FROM globant.departments d
                ),

                employees AS
                (
                SELECT DISTINCT
                    id,
                    id_job,
                    id_deparment,
                    (CASE
                        WHEN QUARTER(hired_date) = 1 THEN 'Q1'
                        WHEN QUARTER(hired_date) = 2 THEN 'Q2'
                        WHEN QUARTER(hired_date) = 3 THEN 'Q3'
                        WHEN QUARTER(hired_date) = 4 THEN 'Q4' END) AS qrt
                FROM globant.employees e
                WHERE YEAR(hired_date) = '2021'
                ),

                jobs AS
                (
                SELECT DISTINCT
                    id,
                    position
                FROM globant.jobs j
                ),

                summary AS (
                SELECT 
                    e.qrt,
                    j.position,
                    d.department,
                    COUNT(e.id) AS conteo
                FROM employees e
                INNER JOIN deparments d
                    ON (e.id_deparment = d.id)
                INNER JOIN jobs j
                    ON (e.id_job = j.id)
                GROUP BY 1,2,3
                    )
                    
                SELECT 
                    department,
                    position,
                    COUNT(CASE WHEN qrt = 'Q1' THEN conteo ELSE 0 END) AS Q1,
                    COUNT(CASE WHEN qrt = 'Q2' THEN conteo ELSE 0 END) AS Q2,
                    COUNT(CASE WHEN qrt = 'Q3' THEN conteo ELSE 0 END) AS Q3,
                    COUNT(CASE WHEN qrt = 'Q4' THEN conteo ELSE 0 END) AS Q4
                FROM summary
                GROUP BY 1,2
                ORDER BY 1,2
                '''
    with engine.connect() as conn, conn.begin():  
        data = pd.read_sql(query, conn)
        print(data)  
    return data.to_json(),200



@employees.post('/hired_employees')
def create_hired_employees():
    current_app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    # check if the post request has the file part
    if 'file' not in request.files:
        return ('not file found',404)
    file = request.files['file']
    # empty file without a filename.
    if file.filename == '':
        return ('No selected file',400)
    try:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(current_app.config['UPLOAD_FOLDER'], filename))
            insert_data()
    except Exception as ex:
        print('Error with the execution of the process',ex)
    return 'hired_employees file has been upload succesful in upload',200

def insert_data():
        
    df_employees = pd.read_csv("uploads/hired_employees.csv", delimiter=",",header=None)
    df_employees.columns = ["id","full_name","hired_date","id_deparment","id_job"]
    df_employees["hired_date"] = pd.to_datetime(df_employees["hired_date"])
    df_employees["id_deparment"] = df_employees["id_deparment"].fillna(0)
    df_employees["id_job"] = df_employees["id_job"].fillna(0)
    df_employees["id_deparment"] = df_employees["id_deparment"].astype(int)
    df_employees["id_job"] = df_employees["id_job"].astype(int)
    print("read employees succesful")

    engine = create_engine("mysql+mysqlconnector://root:1234@localhost/globant".
                        format(user="root",
                                pw="1235",
                                db="globant"))

    df_employees.to_sql('employees', con=engine, if_exists='append', index=False)
    print("insert deparments sucessful")
