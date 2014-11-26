from __future__ import unicode_literals
import time
import json
import datetime
import logging
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
    __publicfields__ = ['job_id', 'description', 'headers', 'results', 'created',
                        'rescheduled', 'submitted', 'finished', 'seen', 'retries',
                        'comp_id']
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
    retries = Column(Integer, default=0)

    comp_id = Column(String(length=40))

    def parse_description(self, key):
        try:
            return dict([x.get('string') for x in json.loads(getattr(self, key)).get('map').get('entry')])
        except:
            return {}

    def to_dict(self):
        d = {}
        for k in self.__publicfields__:
            d[k] = getattr(self, k)
            if k in ('description'):
                description = self.parse_description(k)
                d['analysis_id'] = description.get('analysisID')
            elif k in ('created', 'submitted', 'finished', 'seen', 'rescheduled'):
                if d[k]:
                    try:
                        d[k] = d[k].strftime("%Y-%m-%d %H:%M:%S")
                    except:
                        pass
                    if k in ('submitted'):
                        d['queuing_time'] = (self.submitted - self.created).total_seconds()
                    if k in ('finished'):
                        d['execution_time'] = (self.finished - self.submitted).total_seconds()
        return d

    def __unicode__(self):
        return '<Job:%s>' % self.job_id

    def __str__(self):
        return self.__unicode__()


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
    return job


def update_job_reply_to(session, job_id, reply_to):
    job = session.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise JobNotFound(job_id)
    job.reply_to = reply_to
    session.merge(job)
    return job


def update_job_results(session, job_id, results):
    job = session.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise JobNotFound(job_id)

    if not job.comp_id:
        logging.warn('addings results to job %s with no comp_id' % job_id)

    job.finished = datetime.datetime.utcnow()
    job.results = results
    session.merge(job)
    return job


def update_job_running(session, job_id):
    job = session.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise JobNotFound(job_id)
    job.seen = datetime.datetime.utcnow()
    session.merge(job)
    return job


def update_job_rescheduled(session, job_id):
    job = session.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise JobNotFound(job_id)
    if job.finished:
        raise RuntimeError("cannot reschedule finished job")
    job.rescheduled = datetime.datetime.utcnow()
    job.submitted = job.rescheduled
    job.retries = job.retries + 1
    job.seen = None
    session.merge(job)
    return job


def update_job_cancelled(session, job_id):
    job = session.query(Job).filter(Job.job_id == job_id).first()
    if not job:
        raise JobNotFound(job_id)
    if job.finished:
        raise RuntimeError("cannot cancel completed job")
    job.finished = datetime.datetime.utcnow()
    session.merge(job)
    return job
