import json
from flask import request
from models import Job
from . import app, with_db_session


@app.route("/")
def index():
    return app.send_static_file('index.html')


@app.route("/jobs/<job_id>/results", methods=['GET'])
@with_db_session
def get_job_results(session, job_id):
    job = session.query(Job).filter_by(job_id=job_id).first()
    try:
        results = {}
        data = json.loads(job.results).get('map', {}).get('entry', [])
        for d in data:
            if 'string' in d:
                values = d['string']
                if len(values) == 2:
                    results[values[0]] = values[1]
    except:
        results = []
    return json.dumps(results)


@app.route("/jobs/<job_id>", methods=['GET'])
@with_db_session
def get_job(session, job_id):
    return json.dumps(session.query(Job).filter_by(job_id=job_id).first().to_dict())


@app.route("/jobs/", methods=['GET'])
@with_db_session
def jobs(session):
    active_only = request.args.get('active')
    query = session.query(Job)
    if active_only:
        query = query.filter(Job.finished == None)
    return json.dumps([job.to_dict() for job in query.all()])
