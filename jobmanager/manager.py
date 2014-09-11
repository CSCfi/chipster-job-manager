import argparse
import json
import yaml
import logging
import datetime

from twisted.internet import reactor, defer, task

from stompest.config import StompConfig
from stompest.protocol import StompSpec
from stompest.async import Stomp
from stompest.async.listener import (SubscriptionListener, ErrorListener,
                                     ConnectListener, DisconnectListener, HeartBeatListener)
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from models import (Base, JobNotFound,
                    add_job, get_job, get_jobs, update_job_as,
                    update_job_results, update_job_running,
                    update_job_rescheduled, update_job_reply_to,
                    update_job_cancelled)
from utils import (parse_msg_body, msg_type_from_headers,
                   populate_comp_status_body, populate_headers,
                   populate_comp_status_headers, populate_msg_body,
                   populate_job_running_body, populate_job_result_body,
                   CMD_MESSAGE, RESULT_MESSAGE)

PERIODIC_CHECK_INTERVAL = 5.0
JOB_DEAD_AFTER = 30

TOPICS = {
    'client_topic': '/topic/authorised-request-topic',
    'comp_topic': '/topic/authorized-managed-request-topic',
    'jobmanager_topic': '/topic/jobmanager-topic',
    'comp_admin_topic': '/topic/comp-admin-topic'
}


class ReplyToResolutionException(Exception):
    pass


class JobManagerErrorLister(ErrorListener):
    def onError(self, connection, frame):
        logging.warn("JM AMQ Connection Error")

    def onConnectionLost(self, connection, reason):
        logging.warn("JM AMQ Connection Lost")


def listeners():
    return [ConnectListener(), DisconnectListener(), JobManagerErrorLister(), HeartBeatListener()]


class JobManager(object):

    def __init__(self, session, config=None):
        self.client = None
        self.session = session
        if not config:
            config = StompConfig('tcp://localhost:61613', check=False)
        self.config = config

    @defer.inlineCallbacks
    def run(self):
        self.client = yield Stomp(self.config, listenersFactory=listeners).connect()
        headers = {
            StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL,
            'ack': 'auto',
            'transformation': 'jms-map-json'
        }
        self.client.subscribe(TOPICS['client_topic'], headers,
                              listener=SubscriptionListener(self.processClientMessage, ack=False))
        self.client.subscribe(TOPICS['jobmanager_topic'], headers,
                              listener=SubscriptionListener(self.processCompMessage, ack=False))

    def processClientMessage(self, client, frame):
        try:
            frame_body = json.loads(frame.body)
            body = parse_msg_body(frame_body)
            msg_type = msg_type_from_headers(frame.headers)
            headers = frame.headers
            if msg_type == 'JobMessage':
                topic_name = headers['reply-to'].split('/')[-1]
                reply_to = '/remote-temp-topic/%s' % topic_name
                add_job(self.session, body.get('jobID'), frame.body, json.dumps(frame.headers), headers['session-id'], reply_to=reply_to)
                self.send_to(TOPICS['comp_topic'], headers, json.dumps(frame_body), reply_to=TOPICS['jobmanager_topic'])
            elif msg_type == 'CmdMessage':
                self.handle_client_cmd_msg(frame, body)
            else:
                self.send_to(TOPICS['comp_topic'], headers, frame.body, reply_to=TOPICS['jobmanager_topic'])
        except:
            import traceback
            traceback.print_exc()

    def processCompMessage(self, client, frame):
        frame_body = json.loads(frame.body)
        headers = frame.headers
        body = parse_msg_body(frame_body)
        msg_type = msg_type_from_headers(headers)
        if msg_type == 'ResultMessage':
            try:
                self.handle_result_msg(frame, body)
            except Exception as e:
                logging.error("error in result handling %s" % e)
        elif msg_type == 'StatusMessage':
            # { u'host': u'<hostname>', u'hostId': u'<hostId>'}
            pass
        elif msg_type == 'JobLogMessage':
            self.handle_joblog_msg(headers, body)
        elif msg_type == 'CmdMessage':
            self.handle_comp_cmd_msg(frame, body)
        else:
            try:
                client_topic = self.resolve_reply_to(body)
                self.send_to(client_topic, headers, json.dumps(frame.body), reply_to=headers.get('reply-to'))
            except ReplyToResolutionException:
                logging.warn("unable to resolve reply_to address")

    def handle_result_msg(self, frame, body):
        job_id = body.get('jobId')
        job = get_job(self.session, job_id)
        if body.get('exitState') == 'COMPLETED':
            logging.info("results ready for job %s" % job_id)
            job = update_job_results(self.session, job_id, frame.body)
        logging.info("sending results to %s" % job.reply_to)
        self.send_to(job.reply_to, frame.headers, frame.body)

    def handle_joblog_msg(self, frame, body):
        update_job_running(self.session, body.get('jobId'))

    def handle_comp_cmd_msg(self, frame, msg):
        headers = frame.headers
        command = msg.get('command')
        job_id = msg.get('job-id')
        if command == 'offer':
            job = get_job(self.session, job_id)
            now = datetime.datetime.utcnow()
            schedule_job = False
            if not job.submitted:  # New job, never submitted before
                schedule_job = True
            elif (now - job.created).total_seconds() > JOB_DEAD_AFTER and not job.seen:  # Job has never reported by any analysis server
                schedule_job = True
            elif (now - job.seen).total_seconds() > JOB_DEAD_AFTER:  # The job has not recently been reported by any analysis server
                schedule_job = True
            if schedule_job:
                job_id = job.job_id
                as_id = msg.get('as-id')
                update_job_as(self.session, job_id, as_id)
                body = populate_msg_body('choose', as_id, job_id)
                headers = populate_headers(TOPICS['comp_topic'], CMD_MESSAGE, session_id=job.session_id, reply_to=TOPICS['jobmanager_topic'])
                self.send_to(TOPICS['comp_topic'], headers, body=json.dumps(body))
        else:
            self.send_to(job.reply_to, headers, frame.body)

    def handle_client_cmd_msg(self, frame, msg):
        command = msg.get('command')
        headers = frame.headers
        job_id = msg.get('job-id')
        if command == 'get-job':
            client_topic = headers.get('reply-to')
            job_id = msg.get('job-id')
            headers = populate_headers(client_topic, RESULT_MESSAGE)
            try:
                job = update_job_reply_to(self.session, job_id, client_topic)
                if job.results:
                    resp_body = job.results
                else:
                    resp_body = json.dumps(populate_job_running_body(job.job_id))
            except JobNotFound:
                resp_body = populate_job_result_body(job_id)
            self.send_to(client_topic, headers, resp_body)
        elif command == 'cancel':
            job_id = msg.get('parameter0')
            try:
                update_job_cancelled(self.session, job_id)
            except JobNotFound:
                logging.warn("trying to cancel a non-existing job")
            self.send_to(TOPICS['comp_topic'], headers, frame.body)
        else:
            self.send_to(TOPICS['comp_topic'], headers, frame.body)

    def send_to(self, destination, headers, body, reply_to=None):
        headers[u'destination'] = destination
        if reply_to:
            headers[u'reply-to'] = reply_to
        if not self.client:
            logging.warn("unable to send, client not ready")
            raise Exception("client not ready")
        self.client.send(destination=destination, headers=headers, body=body)

    def resolve_reply_to(self, body):
        if 'job-id' in body:
            job = get_job(self.session, body.get('job-id'))
            if job:
                return job.reply_to
        raise ReplyToResolutionException('Unable to resolve reply_to address')

    def schedule_job(self, session, job_id):
        job = get_job(session, job_id)
        headers = json.loads(job.headers)
        self.send_to(TOPICS['comp_topic'], headers, job.description, reply_to=TOPICS['jobmanager_topic'])
        update_job_rescheduled(session, job_id)

    def run_periodic_checks(self, session):
        if self.client:
            status_headers = populate_comp_status_headers(TOPICS['jobmanager_topic'])
            status_body = populate_comp_status_body('get-comp-status')
            self.send_to(TOPICS['comp_admin_topic'], status_headers, json.dumps(status_body))
            jobs_headers = populate_comp_status_headers(TOPICS['jobmanager_topic'])
            jobs_body = populate_comp_status_body('get-running-jobs')
            self.send_to(TOPICS['comp_admin_topic'], jobs_headers, json.dumps(jobs_body))
            self.check_stalled_jobs(session)

    def check_stalled_jobs(self, session):
        now = datetime.datetime.utcnow()
        for job in get_jobs(session):
            if job.submitted and job.seen:
                if (now - job.seen).total_seconds() > JOB_DEAD_AFTER:
                    logging.warn("Job %s seems to be dead, rescheduling job" %
                                 job.job_id)
                    self.schedule_job(session, job.job_id)
            elif job.submitted and not job.seen:
                if (now - job.submitted).total_seconds() > JOB_DEAD_AFTER:
                    logging.warn("Job %s is not reported by any analysis "
                                 "server and is not expired, rescheduling "
                                 "job" % job.job_id)
                    self.schedule_job(session, job.job_id)
            elif job.created and not job.submitted:
                if (now - job.created).total_seconds() > JOB_DEAD_AFTER:
                    logging.warn("Job %s is not scheduled and is now "
                                 "expired, rescheduling" % job.job_id)
                    self.schedule_job(session, job.job_id)


def config_to_db_session(config, Base):
    if config['database_dialect'] == 'sqlite':
        connect_string = 'sqlite://%s' % config['database_connect_string']
    engine = create_engine(connect_string)

    if config['database_dialect'] == 'sqlite':
        def _fk_pragma_on_connect(dbapi_con, con_record):
            cursor = dbapi_con.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
        event.listen(engine, 'connect', _fk_pragma_on_connect)
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine)()


def main():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    config = yaml.load(open(args.config_file))
    session = config_to_db_session(config, Base)

    stomp_endpoint = config['stomp_endpoint']
    stomp_login = config['stomp_login']
    stomp_password = config['stomp_password']

    stomp_config = StompConfig(stomp_endpoint, login=stomp_login, passcode=stomp_password)

    for topic in ['jobmanager_topic', 'client_topic', 'comp_topic', 'comp_admin_topic']:
        if topic in config:
            TOPICS[topic] = config[topic]
    jm = JobManager(session, config=stomp_config)
    jm.run()

    l = task.LoopingCall(jm.run_periodic_checks, session)
    l.start(PERIODIC_CHECK_INTERVAL)

    reactor.run()


if __name__ == '__main__':
    main()
