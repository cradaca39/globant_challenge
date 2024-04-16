import os
from flask import request, Blueprint, current_app
from werkzeug.utils import secure_filename
import pandas as pd
from sqlalchemy import create_engine
from database_connect import Database

UPLOAD_FOLDER = './uploads'
ALLOWED_EXTENSIONS = {'csv'}

departments = Blueprint("departments", __name__)

def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@departments.post('/departments')
def create_departments():

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
    return 'departments file has been upload succesful in upload',200

def insert_data():
        
    df = pd.read_csv("uploads/departments.csv", delimiter=",",header=None)
    df.columns = ["id","department"]
    print("read departments succesful")
    
    db = Database()
    engine = db.connection()


    df.to_sql('departments', con=engine, if_exists='append', index=False)
    print("insert deparments sucessful")
