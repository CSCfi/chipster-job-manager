import json
import time
import uuid

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

JOB_MESSAGE = 'fi.csc.microarray.messaging.message.JobMessage'
CMD_MESSAGE = 'fi.csc.microarray.messaging.message.CommandMessage'
RESULT_MESSAGE = 'fi.csc.microarray.messaging.message.ResultMessage'
STATUS_MESSAGE = 'fi.csc.microarray.messaging.message.ServerStatusMessage'
JOBLOG_MESSAGE = 'fi.csc.microarray.messaging.message.JobLogMessage'
JSON_MESSAGE = 'fi.csc.microarray.messaging.message.JsonMessage'

def populate_headers(destination, msg_class, session_id='null', timestamp=None,
                     reply_to=None):
    timestamp = str(int(time.time()) * 1000)
    message_id = '%s' % uuid.uuid4()
    msg = {u'username': u'chipster',
           u'session-id': session_id,
           u'destination': destination,
           u'timestamp': timestamp,
           u'expires': u'0',
           u'persistent': u'true',
           u'class': msg_class,
           u'priority': u'4',
           u'multiplex-channel': u'reply-to',
           u'message-id': u'%s' % message_id,
           u'transformation': u'jms-map-json'}
    if reply_to:
        msg[u'reply-to'] = reply_to
    return msg



def populate_comp_status_headers(reply_topic, timestamp=None):
    timestamp = str(int(time.time()) * 1000)
    message_id = '%s' % uuid.uuid4()
    return {u'username': u'null',
            u'session-id': u'null',
            u'destination': u'/topic/comp-admin-topic',
            u'timestamp': '%s' % timestamp,
            u'expires': u'0',
            u'persistent': u'true',
            u'class': u'fi.csc.microarray.messaging.message.CommandMessage',
            u'priority': u'4',
            u'multiplex-channel': u'null',
            u'reply-to': reply_topic,
            u'message-id': message_id,
            u'transformation': u'jms-map-json'
            }

def populate_msg_body(msg_type, as_id, job_id):
    if msg_type == 'choose':
        return ('{"map": {"entry": ['
                '{"string": ["command", "choose"]},'
                '{"string": ["named-parameter-value1", "%s"]},'
                '{"string": ["named-parameter-key1", "as-id"]},'
                '{"string": ["named-parameter-value0", "%s"]},'
                '{"string": ["named-parameter-key0", "job-id"]}'
                ']}}' % (as_id, job_id))
    elif msg_type == 'offer':
        return ('{"map": {"entry": ['
                '{"string": ["command", "offer"]},'
                '{"string": ["named-parameter-value1", "%s"]},'
                '{"string": ["named-parameter-key1", "as-id"]},'
                '{"string": ["named-parameter-value0", "%s"]},'
                '{"string": ["named-parameter-key0", "job-id"]}'
                ']}}' % (as_id, job_id))
    else:
        assert RuntimeError("unknown message type")


def populate_comp_status_body(command):
    return '{"map": {"entry": {"string": ["command", "%s"]}}}' % command


def populate_cancel_body(job_id):
    return ('{"map": {"entry": [{"string": ["command", "cancel"]},'
            '{"string":["parameter0", "%s"]}]}}' % job_id)


def populate_joblog_body(jobs):
    def parse_joblog_fields(job):
        error_message = ''
        state_detail = ''
        output_text = ''
        finished = ''
        results = {}
        if job.results:
            try:
                results = parse_msg_body(json.loads(job.results))
            except:
                pass
        if job.finished:
            try:
                finished = job.finished.strftime("%b %d, %Y %I:%M:%S %p")
            except:
                pass

        error_message = results.get('errorMessage', '')
        state_detail = results.get('stateDetail', '')
        output_text = results.get('outputText', '')

        return {'errorMessage': error_message,
                'startTime': job.created.strftime("%b %d, %Y %I:%M:%S %p"),
                'operation': job.analysis_id,
                'username': job.username,
                'outputText': output_text,
                'jobId': job.job_id,
                'compHost': job.comp_id,
                'endTime': finished,
                'stateDetail': state_detail,
                'exitState': job.state}

    return ('{"map":{"entry":{"string":["json", %s]}}}' %
            json.dumps(json.dumps([parse_joblog_fields(job) for job in jobs])))


def populate_job_running_body(job_id):
    # AMQ Stomp message transformation assumes dict fields in certain order
    # which is why this message is not serialized through json library
    return ('{"map": {"entry": [{"string": ["jobId", "%s"]},'
            '{"string": "heartbeat", "boolean": "false"},'
            '{"string": ["exitState", "RUNNING"]}]}}' % job_id)


def populate_job_result_body(job_id, exit_state='ERROR', error_msg=''):
    # AMQ Stomp message transformation assumes dict fields in certain order
    # which is why this message is not serialized through json library
    error_field_type = 'null'
    if error_msg:
        error_field_type = 'string'
    return ('{"map": {"entry": [{"string": "errorMessage","%s": "%s"},'
            '{"string":"heartbeat","boolean":"false"},'
            '{"string": ["jobId", "%s"]},'
            '{"string": ["exitState", "%s"]}]}}' % (error_field_type, error_msg, job_id, exit_state))


def populate_service_status_reply(host):
    return {"map": {"entry": [{"string": ["command", "ping-reply"]}, {"string": ["parameter1", host]}, {"string": ["parameter0", "jobmanager"]}]}}


def msg_type_from_headers(headers):
    if headers.get('class') == JOB_MESSAGE:
        return 'JobMessage'
    elif headers.get('class') == CMD_MESSAGE:
        return 'CmdMessage'
    elif headers.get('class') == RESULT_MESSAGE:
        return 'ResultMessage'
    elif headers.get('class') == STATUS_MESSAGE:
        return 'StatusMessage'
    elif headers.get('class') == JOBLOG_MESSAGE:
        return 'JobLogMessage'
    else:
        return None


def parse_msg_body(msg):
    """
    Filter and normalize messages to std Python dictionaries
    """
    entry = msg['map']['entry']
    if type(entry) is dict:
        return entry

    bindings = (x['string'] for x in entry)
    extras = {}
    tmp = {}
    result = None
    for element in bindings:
        if len(element) == 2:
            metavar, value = element
        else:
            continue

        metavar_type, metavar_seq = metavar[:-1], metavar[-1]

        if metavar_type not in ('named-parameter-key',
                                'named-parameter-value'):
            extras[metavar] = value
            continue

        cell_idx = None
        if metavar_type == 'named-parameter-key':
            cell_idx = 0
        elif metavar_type == 'named-parameter-value':
            cell_idx = 1
        cell = tmp.get(metavar_seq, [None, None])
        assert not cell[cell_idx]
        cell[cell_idx] = value
        tmp[metavar_seq] = cell
    result = dict(tmp.values())
    for k, v in extras.items():
        assert k not in result
        result[k] = v
    return result


def parse_description(desc):
    try:
        return dict([x.get('string') for x in json.loads(desc).get('map').get('entry')])
    except:
        return {}


def config_to_db_session(config, Base):
    if config['database_dialect'] == 'sqlite':
        connect_string = 'sqlite:///%s' % config['database_connect_string']
    engine = create_engine(connect_string)

    if config['database_dialect'] == 'sqlite':
        def _fk_pragma_on_connect(dbapi_con, con_record):
            cursor = dbapi_con.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
        event.listen(engine, 'connect', _fk_pragma_on_connect)
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
