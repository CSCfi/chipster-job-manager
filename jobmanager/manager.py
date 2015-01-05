import argparse
import json
import yaml
import uuid
import logging
import socket
import datetime

from logging import FileHandler
from contextlib import contextmanager
from twisted.internet import reactor, defer, task

from stompest.config import StompConfig
from stompest.protocol import StompSpec
from stompest.async import Stomp
from stompest.async.listener import (SubscriptionListener, ErrorListener,
                                     ConnectListener, DisconnectListener,
                                     HeartBeatListener)
from stompest.sync import Stomp as StompSync
from jobmanager.models import (Base, JobNotFound,
                               add_job, get_job, get_jobs, update_job_comp,
                               update_job_results, update_job_running,
                               update_job_rescheduled, update_job_reply_to,
                               update_job_cancelled, purge_completed_jobs)
from jobmanager.utils import (parse_msg_body, msg_type_from_headers,
                              populate_comp_status_body, populate_headers,
                              populate_comp_status_headers, populate_msg_body,
                              populate_job_running_body, populate_cancel_body,
                              populate_job_result_body, config_to_db_session,
                              populate_joblog_body, CMD_MESSAGE,
                              RESULT_MESSAGE, JSON_MESSAGE)

PERIODIC_CHECK_INTERVAL = 5.0
JOB_DEAD_AFTER = 60
MAX_JOB_RETRIES = 3

TOPICS = {
    'client_topic': '/topic/authorised-request-topic',
    'comp_topic': '/topic/authorized-managed-request-topic',
    'jobmanager_topic': '/topic/jobmanager-topic',
    'jobmanager_admin_topic': '/topic/jobmanager-admin-topic',
    'comp_admin_topic': '/topic/comp-admin-topic',
    'admin_topic': '/topic/admin-topic',
}


class ReplyToResolutionException(Exception):
    pass


class JobManagerErrorLister(ErrorListener):
    # E.g. trying to send to disconnected client
    def onError(self, connection, frame):
        logging.warn("JM AMQ Connection Error")

    # E.g. AMQ connection problem
    def onConnectionLost(self, connection, reason):
        logging.warn("JM AMQ Connection Lost")
        reactor.callFromThread(reactor.stop)


def listeners():
    return [ConnectListener(), DisconnectListener(), JobManagerErrorLister(),
            HeartBeatListener()]


class JobManager(object):

    def __init__(self, sessionmaker, config=None):
        self.client = None
        self.sessionmaker = sessionmaker
        if not config:
            config = StompConfig('tcp://localhost:61613', check=False)
        self.config = config

    @defer.inlineCallbacks
    def run(self):
        try:
            self.client = yield Stomp(self.config,
                                      listenersFactory=listeners).connect()
        except:
            reactor.stop()
        headers = {
            StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL,
            'ack': 'auto',
            'transformation': 'jms-map-json'
        }

        self.client.subscribe(
            TOPICS['client_topic'],
            headers,
            listener=SubscriptionListener(self.process_client_message, ack=False))
        self.client.subscribe(
            TOPICS['jobmanager_topic'],
            headers,
            listener=SubscriptionListener(self.process_comp_message, ack=False))
        self.client.subscribe(
            TOPICS['jobmanager_admin_topic'],
            headers,
            listener=SubscriptionListener(self.process_jobmanager_admin_message, ack=False))
        self.client.subscribe(
            TOPICS['admin_topic'],
            headers,
            listener=SubscriptionListener(self.process_admin_message, ack=False))

    def process_client_message(self, client, frame):
        try:
            msg_type = msg_type_from_headers(frame.headers)
            if msg_type == 'JobMessage':
                self.handle_client_job_msg(frame)
            elif msg_type == 'CmdMessage':
                self.handle_client_cmd_msg(frame)
            else:
                self.send_to(TOPICS['comp_topic'], frame.headers, frame.body,
                             reply_to=TOPICS['jobmanager_topic'])
        except Exception as e:
            logging.warn(e)

    def process_comp_message(self, client, frame):
        frame_body = json.loads(frame.body)
        headers = frame.headers
        body = parse_msg_body(frame_body)
        msg_type = msg_type_from_headers(headers)
        if msg_type == 'ResultMessage':
            self.handle_result_msg(frame, body)
        elif msg_type == 'JobLogMessage':
            self.handle_joblog_msg(headers, body)
        elif msg_type == 'CmdMessage':
            self.handle_comp_cmd_msg(frame, body)
        elif msg_type == 'StatusMessage':
            pass
        else:
            try:
                client_topic = self.resolve_reply_to(body)
                logging.warn("unknown msg: %s" % frame.body)
                self.send_to(client_topic, headers, json.dumps(frame.body),
                             reply_to=headers.get('reply-to'))
            except ReplyToResolutionException:
                logging.warn("unable to resolve reply_to address")

    def process_admin_message(self, client, frame):
        admin_topic = TOPICS['admin_topic']
        headers = populate_headers(admin_topic, CMD_MESSAGE)
        try:
            frame_body = json.loads(frame.body)
            body = parse_msg_body(frame_body)
            content = body.get('string')
            if content:
                msg_type, msg_command = content
        except Exception as e:
            logging.warn(e)
            return

        if msg_command == 'ping':
            reply = populate_service_status_reply(socket.gethostname())
            self.send_to(admin_topic, headers=headers, body=reply)

    def process_jobmanager_admin_message(self, client, frame):
        msg_type = msg_type_from_headers(frame.headers)
        if msg_type == 'CmdMessage':
            self.handle_admin_cmd_msg(frame)

    def handle_admin_cmd_msg(self, frame):
        reply_to = None
        try:
            topic_name = frame.headers['reply-to'].split('/')[-1]
            reply_to = '/remote-temp-topic/%s' % topic_name
        except Exception as e:
            pass

        try:
            frame_body = json.loads(frame.body)
            msg = parse_msg_body(frame_body)
        except Exception as e:
            logging.warn('unable to parse frame body: %s' % e)

        command = msg.get('command')
        # XXX: Admin Web messages have no standard schema, if no command can be
        # found in message try a bit harder...
        if not command:
            try:
                check, command = msg.get('string')
                if check != 'command':
                    logging.warn('invalid message: %s' % msg)
                    return
            except:
                logging.warn('invalid message: %s' % msg)
                return

        if command == 'get-status-report':
            try:
                headers = populate_headers(reply_to, JSON_MESSAGE)
                if not reply_to:
                    logging.warn("get status report failed: no valid reply_to topic in request")
                    return

                self.self_to(reply_to, headers=headers,
                        body='{"map":{"entry":{"string":["json","ok"]}}}')
            except Exception as e:
                logging.warn("get status report failed: %s" % e)
        elif command == 'purge-old-jobs':
            try:
                self.purge_old_jobs()
            except Exception as e:
                logging.warn("purge old jobs failed: %s" % e)
        elif command == 'get-running-jobs':
            if not reply_to:
                logging.warn("get running jobs failed: no valid reply_to topic in request")
                return

            try:
                headers = populate_headers(reply_to, JSON_MESSAGE)
                with self.session_scope() as session:
                    self.send_to(reply_to, headers=headers,
                                 body=populate_joblog_body(get_jobs(session)))
            except Exception as e:
                logging.warn("get running jobs failed: %s" % e)
        elif command == 'cancel':
            try:
                job_id = parse_msg_body(frame_body).get('job-id')
                session_id = frame.headers.get('session-id')
                self.cancel_job(job_id, session_id)
                with self.session_scope() as session:
                    job = get_job(session, job_id)
                if job.reply_to:
                    headers = populate_headers(job.reply_to, RESULT_MESSAGE)
                    reply_body = populate_job_result_body(job_id, exit_state='CANCELLED')
                    self.send_to(job.reply_to, headers=headers, body=reply_body)
            except Exception as e:
                logging.warn("cancel job failed: %s" % e)

    def handle_result_msg(self, frame, body):
        job_id = body.get('jobId')
        with self.session_scope() as session:
            job = get_job(session, job_id)
            exit_state = body.get('exitState')
            if exit_state == 'COMPLETED':
                logging.info("results ready for job %s" % job_id)
                job = update_job_results(session, job_id, frame.body, exit_state)
            elif exit_state == 'ERROR':
                logging.info("job %s in error state %s" % (job_id, body))
                job = update_job_results(session, job_id, frame.body, exit_state)
            elif exit_state == 'RUNNING':
                logging.info("heartbeat for job %s" % job_id)
                job = update_job_running(session, job_id)
            elif exit_state == 'FAILED':
                logging.info("job %s failed" % job_id)
                try:
                    update_job_results(session, job_id, frame.body, exit_state)
                except Exception as e:
                    logging.warn(e)
            elif exit_state == 'FAILED_USER_ERROR':
                logging.info("job %s in error state %s" %
                             (job_id, exit_state))
                job = update_job_results(session, job_id, frame.body, exit_state)
        logging.info("job %s in state %s sending results to %s" %
                     (job_id, exit_state, job.reply_to))
        self.send_to(job.reply_to, frame.headers, frame.body)

    def handle_joblog_msg(self, frame, body):
        with self.session_scope() as session:
            update_job_running(session, body.get('jobId'))

    def handle_comp_cmd_msg(self, frame, msg):
        headers = frame.headers
        command = msg.get('command')
        job_id = msg.get('job-id')
        if command == 'offer':
            with self.session_scope() as session:
                job = get_job(session, job_id)
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
                with self.session_scope() as session:
                    update_job_comp(session, job_id, as_id)
                body = populate_msg_body('choose', as_id, job_id)
                headers = populate_headers(TOPICS['comp_topic'], CMD_MESSAGE,
                                           session_id=job.session_id,
                                           reply_to=TOPICS['jobmanager_topic'])
                self.send_to(TOPICS['comp_topic'], headers, body=body)
        else:
            self.send_to(job.reply_to, headers, frame.body)

    def handle_client_job_msg(self, frame):
        frame_body = json.loads(frame.body)
        body = parse_msg_body(frame_body)
        topic_name = frame.headers['reply-to'].split('/')[-1]
        reply_to = '/remote-temp-topic/%s' % topic_name
        job_id = body.get('jobID')
        session_id = frame.headers['session-id']
        self.schedule_job(job_id, frame.headers, frame.body,
                          session_id=session_id, reply_to=reply_to)

    def handle_client_cmd_msg(self, frame):
        frame_body = json.loads(frame.body)
        msg = parse_msg_body(frame_body)
        command = msg.get('command')
        headers = frame.headers
        job_id = msg.get('job-id')
        if command == 'get-job':
            client_topic = headers.get('reply-to')
            job_id = msg.get('job-id')
            headers = populate_headers(client_topic, RESULT_MESSAGE)
            with self.session_scope() as session:
                try:
                    job = update_job_reply_to(session, job_id, client_topic)
                    if job.finished and job.results:
                        resp_body = job.results
                    elif job.finished:
                        resp_body = populate_job_result_body(job_id, exit_state='CANCELLED')
                    else:
                        resp_body = populate_job_running_body(job.job_id)
                except JobNotFound:
                    logging.info("job %s not found" % job_id)
                    resp_body = populate_job_result_body(job_id, error_msg='job not found')
            self.send_to(client_topic, headers, resp_body)
        elif command == 'cancel':
            job_id = msg.get('parameter0')
            session_id = headers.get('session-id')
            self.cancel_job(job_id, session_id)
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
            with self.session_scope() as session:
                job = get_job(session, body.get('job-id'))
                if job:
                    return job.reply_to
        raise ReplyToResolutionException('Unable to resolve reply_to address')

    def schedule_job(self, job_id, headers, body, session_id, reply_to):
        with self.session_scope() as session:
            add_job(session, job_id, body, json.dumps(headers), session_id,
                    reply_to=reply_to)
        self.send_to(TOPICS['comp_topic'], headers, body,
                     reply_to=TOPICS['jobmanager_topic'])

    def reschedule_job(self, job_id):
        with self.session_scope() as session:
            job = get_job(session, job_id)
            if job.retries >= MAX_JOB_RETRIES:
                job.finished = datetime.datetime.utcnow()
                job.results = populate_job_result_body(job_id, error_msg='maximum number of job submits exceeded, available chipster-comps cannot run the job')
                headers = populate_headers(job.reply_to, RESULT_MESSAGE)
                self.send_to(job.reply_to, headers, job.results)
            else:
                headers = json.loads(job.headers)
                self.send_to(TOPICS['comp_topic'], headers, job.description, reply_to=TOPICS['jobmanager_topic'])
                update_job_rescheduled(session, job_id)

    def cancel_job(self, job_id, session_id):
        headers = populate_headers(TOPICS['comp_topic'], CMD_MESSAGE,
                                   session_id=session_id)
        body = populate_cancel_body(job_id)
        with self.session_scope() as session:
            try:
                update_job_cancelled(session, job_id)
            except JobNotFound:
                logging.warn("trying to cancel a non-existing job")
        self.send_to(TOPICS['comp_topic'], headers, body)

    def purge_old_jobs(self):
        with self.session_scope() as session:
            purge_completed_jobs(session)

    def run_periodic_checks(self):
        if self.client:
            status_headers = populate_comp_status_headers(TOPICS['jobmanager_topic'])
            status_body = populate_comp_status_body('get-comp-status')
            self.send_to(TOPICS['comp_admin_topic'], status_headers, status_body)
            jobs_headers = populate_comp_status_headers(TOPICS['jobmanager_topic'])
            jobs_body = populate_comp_status_body('get-running-jobs')
            self.send_to(TOPICS['comp_admin_topic'], jobs_headers, jobs_body)
            self.check_stalled_jobs()

    def check_stalled_jobs(self):
        now = datetime.datetime.utcnow()
        with self.session_scope() as session:
            for job in get_jobs(session):
                if job.submitted and job.seen:
                    if (now - job.seen).total_seconds() > JOB_DEAD_AFTER:
                        logging.warn("Job %s seems to be dead (no action for %s seconds), rescheduling job" %
                                     (job.job_id, (now - job.seen).total_seconds()))
                        self.reschedule_job(job.job_id)
                elif job.submitted and not job.seen:
                    if (now - job.submitted).total_seconds() > JOB_DEAD_AFTER:
                        logging.warn("Job %s is not reported by any analysis "
                                     "server and is not expired, rescheduling "
                                     "job" % job.job_id)
                        self.reschedule_job(job.job_id)
                elif job.created and not job.submitted:
                    if (now - job.created).total_seconds() > JOB_DEAD_AFTER:
                        logging.warn("Job %s is not scheduled and is now "
                                     "expired, rescheduling" % job.job_id)
                        self.reschedule_job(job.job_id)

    @contextmanager
    def session_scope(self):
        session = self.sessionmaker()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.expunge_all()
            session.close()


def generate_load(client):
    job_id = uuid.uuid4().hex
    session_id = "91788a65-57f9-41ed-8a0e-0302e16e2eed"
    headers = {u'username': u'chipster', u'session-id': session_id, u'destination': TOPICS['comp_topic'], u'timestamp': u'1417607038606', u'expires': u'0', u'persistent': u'true', u'class': u'fi.csc.microarray.messaging.message.JobMessage', u'priority': u'4', u'multiplex-channel': u'null', u'reply-to': TOPICS['jobmanager_topic'], u'message-id': '%s' % uuid.uuid4().hex, u'transformation': u'jms-map-json', u'reply-to': TOPICS['jobmanager_topic']}
    body = {"map":{"entry":[{"string":["payload_input","9def6449-766c-4a73-a3b2-0a3018d00f78"]},{"string":["analysisID","test-data-in.R"]},{"string":["jobID", job_id]}]}}
    client.send(TOPICS['comp_topic'], headers=headers, body=json.dumps(body))


def main():
    logging.basicConfig(format='%(asctime)s %(message)s')
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--logfile')
    parser.add_argument('--purge', action='store_true')
    parser.add_argument('--loadgen', action='store_true')
    args = parser.parse_args()

    if args.logfile:
        logging.getLogger().addHandler(FileHandler(args.logfile))

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    config = yaml.load(open(args.config_file))
    sessionmaker = config_to_db_session(config, Base)

    if args.purge:
        purge_completed_jobs(sessionmaker())
        return

    stomp_endpoint = config['stomp_endpoint']
    stomp_login = config['stomp_login']
    stomp_password = config['stomp_password']

    stomp_config = StompConfig(stomp_endpoint, login=stomp_login, passcode=stomp_password)

    for topic in ['jobmanager_topic', 'client_topic', 'comp_topic', 'comp_admin_topic']:
        if topic in config:
            TOPICS[topic] = config[topic]

    if args.loadgen:
        jm = StompSync(stomp_config)
        jm.connect()
        generate_load(jm)
        jm.disconnect()
        return

    jm = JobManager(sessionmaker, config=stomp_config)
    jm.run()

    l = task.LoopingCall(jm.run_periodic_checks)
    l.start(PERIODIC_CHECK_INTERVAL)

    reactor.run()


if __name__ == '__main__':
    main()
