import datetime

from sqlalchemy import (Column, Integer, String, Text,
                        DateTime)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class JobNotFound(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = 'job %s does not exists' % message


class Job(Base):
    __tablename__ = 'jobs'

    id = Column(Integer, primary_key=True)
    job_id = Column(String(length=40))
    session_id = Column(String(length=40))
    description = Column(Text)
    headers = Column(Text)
    results = Column(Text)
    reply_to = Column(String(length=255))

    created = Column(DateTime)
    rescheduled = Column(DateTime)
    submitted = Column(DateTime)
    finished = Column(DateTime)
    seen = Column(DateTime)

    comp_id = Column(String(length=40))


def get_jobs(session, include_finished=False):
    if include_finished:
        return session.query(Job).all()
    else:
        return session.query(Job).filter(Job.finished == None)


def get_job(session, job_id):
    job = session.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise JobNotFound(job_id)
    return job


def add_job(session, job_id, description, headers, session_id, reply_to=None):
    desc = {}
    desc['job_id'] = job_id
    desc['description'] = description
    desc['created'] = datetime.datetime.utcnow()
    desc['headers'] = headers
    desc['reply_to'] = reply_to
    desc['session_id'] = session_id
    job = Job(**desc)
    session.add(job)
    session.commit()
    return job


def update_job_comp(session, job_id, comp_id):
    job = session.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise JobNotFound(job_id)
    if job.results:
        raise ValueError('cannot modify finished job: %s' % job_id)
    job.submitted = datetime.datetime.utcnow()
    job.comp_id = comp_id
    session.merge(job)
    session.commit()
    return job


def update_job_reply_to(session, job_id, reply_to):
    job = session.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise JobNotFound(job_id)
    job.reply_to = reply_to
    session.merge(job)
    session.commit()
    return job


def update_job_results(session, job_id, results):
    job = session.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise JobNotFound(job_id)

    if not job.comp_id:
        raise ValueError('cannot add results to job with no as_id' % job_id)

    job.finished = datetime.datetime.utcnow()
    job.results = results
    session.merge(job)
    session.commit()
    return job


def update_job_running(session, job_id):
    job = session.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise JobNotFound(job_id)
    job.seen = datetime.datetime.utcnow()
    session.merge(job)
    session.commit()
    return job


def update_job_rescheduled(session, job_id):
    job = session.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise JobNotFound(job_id)
    if job.finished:
        raise RuntimeError("cannot reschedule finished job")
    job.rescheduled = datetime.datetime.utcnow()
    job.submitted = job.rescheduled
    job.seen = None
    session.merge(job)
    session.commit()
    return job


def update_job_cancelled(session, job_id):
    job = session.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise JobNotFound(job_id)
    if job.finished:
        raise RuntimeError("cannot cancel completed job")
    job.finished = datetime.datetime.utcnow()
    session.merge(job)
    session.commit()
    return job
