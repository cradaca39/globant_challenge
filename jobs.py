import os
from flask import request, Blueprint, current_app
from werkzeug.utils import secure_filename
import pandas as pd
from sqlalchemy import create_engine
from database_connect import Database


UPLOAD_FOLDER = './uploads'
ALLOWED_EXTENSIONS = {'csv'}

jobs = Blueprint("jobs", __name__)

def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@jobs.post('/jobs')
def create_jobs():
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
    return 'jobs file has been upload succesful in upload',200
    

def insert_data():
    
    df_jobs = pd.read_csv("uploads/jobs.csv", delimiter=",",header=None)
    df_jobs.columns = ["id","position"]
    print("read departments succesful")

    db = Database()
    engine = db.connection()

    df_jobs.to_sql('jobs', con=engine, if_exists='append', index=False)
    print("insert jobs sucessful")