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
    query = ''' select 
                    d.id, 
                    d.department, 
                    count(e.id) as hired
                from globant.employees e 
                join globant.departments d 
                on (e.id_deparment = d.id)
                where substr(e.hired_date,1,4) = '2021' 
                group by department
                having hired > (select count(e.id)/12
                                from globant.employees e 
                                where substr(e.hired_date,1,4) = '2021'
                            )
                order by hired desc
 '''
    with engine.connect() as conn, conn.begin():  
        data = pd.read_sql(query, conn)
        print(data)  
    return data.to_json(orient="values"),200

@employees.get('/jobs-by-quarter')
def jobs_by_quarter_query():
    database = Database()
    engine = database.connection()
    query = ''' select 	
                    department, 
                    position
                    ,count(case when substr(e.hired_date,6,2) in ('01','02','03') then e.id end) as Q1
                    ,count(case when substr(e.hired_date,6,2) in ('04','05','06') then e.id end) as Q2
                    ,count(case when substr(e.hired_date,6,2) in ('07','08','09') then e.id end) as Q3
                    ,count(case when substr(e.hired_date,6,2) in ('10','11','12') then e.id end) as Q4
                from
                globant.employees e
                join globant.jobs j 
                    on (e.id_job = j.id)
                join globant.departments d 
                    on (e.id_deparment = d.id)
                where substr(e.hired_date,1,4) = '2021'
                group by position, department
                order by department, position
                '''
    with engine.connect() as conn, conn.begin():  
        data = pd.read_sql(query, conn)
        print(data)  
    return data.to_json(orient="values"),200



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

    db = Database()
    engine = db.connection()
    
    df_employees.to_sql('employees', con=engine, if_exists='append', index=False)
    print("insert deparments sucessful")
