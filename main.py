from departments import departments
from jobs import jobs
from hired_employees import employees
from flask import Flask


if __name__ == "__main__":

    try:
        app = Flask(__name__)
        app.register_blueprint(departments)
        app.register_blueprint(employees)
        app.register_blueprint(jobs)
        app.run(debug=True)
    except Exception as e:
        print('Error with the execution of the process',e)
