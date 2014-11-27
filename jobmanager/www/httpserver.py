import json
import os
from flask import jsonify, request
from models import Job, purge_completed_jobs
from . import app, with_db_session


@app.route("/")
def index():
    return app.send_static_file('index.html')


@app.route("/jobs/<job_id>/results", methods=['GET'])
@with_db_session
def get_job_results(session, job_id):
    job = session.query(Job).filter_by(job_id=job_id).first()
    results = {}
    try:
        data = json.loads(job.results).get('map', {}).get('entry', [])
        for d in data:
            if 'string' in d:
                values = d['string']
                if len(values) == 2:
                    results[values[0]] = values[1]
    except:
        pass
    results['job_id'] = job.job_id
    results['state'] = job.state
    return jsonify(results)


@app.route("/jobs/<job_id>", methods=['GET'])
@with_db_session
def get_job(session, job_id):
    return jsonify(session.query(Job).filter_by(job_id=job_id).first().to_dict())


@app.route("/jobs/", methods=['GET'])
@with_db_session
def jobs(session):
    active_only = request.args.get('active')
    query = session.query(Job)
    if active_only:
        query = query.filter(Job.finished == None)
    return json.dumps([job.to_dict() for job in query.all()])

@app.route("/jobs/", methods=['DELETE'])
@with_db_session
def purge_jobs(session):
    purge_completed_jobs(session)
    return jsonify({'result': True})

@app.route('/system_info/', methods=['GET'])
def system_info():
    if not app.config['params']:
        return jsonify({'result': False, 'error_string': 'config parameters missing'})

    info = {}
    if app.config['params']['database_dialect'] == 'sqlite':
        db_size = os.path.getsize(app.config['params']['database_connect_string'])
        info['db_size'] = db_size / 1000

    info['result'] = True
    return jsonify(info)

