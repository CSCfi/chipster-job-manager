import json
from flask import Flask, render_template
from models import Job
from . import app, with_db_session


@app.route("/")
def index():
    return app.send_static_file('index.html')

@app.route("/jobs/<job_id>", methods=['GET'])
@with_db_session
def get_job(session):
    return json.dumps(session.query(Job).filter_by(job_id=job_id))

@app.route("/jobs/", methods=['GET'])
@with_db_session
def jobs(session):

    return json.dumps([job.to_dict() for job in session.query(Job).all()])
