import os
from flask import request, Blueprint, current_app
from werkzeug.utils import secure_filename
import pandas as pd
from sqlalchemy import create_engine



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
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file.save(os.path.join(current_app.config['UPLOAD_FOLDER'], filename))
        insert_data()
    return 'jobs file has been upload succesful in upload',200

def insert_data():
        
    df_jobs = pd.read_csv("uploads/jobs.csv", delimiter=",",header=None)
    df_jobs.columns = ["id","position"]
    print("read departments succesful")

    engine = create_engine("mysql+mysqlconnector://root:1234@localhost/globant".
                        format(user="root",
                                pw="1235",
                                db="globant"))

    df_jobs.to_sql('jobs', con=engine, if_exists='append', index=False)
    print("insert jobs sucessful")