import time
import uuid

JOB_MESSAGE = 'fi.csc.microarray.messaging.message.JobMessage'
CMD_MESSAGE = 'fi.csc.microarray.messaging.message.CommandMessage'
RESULT_MESSAGE = 'fi.csc.microarray.messaging.message.ResultMessage'
STATUS_MESSAGE = 'fi.csc.microarray.messaging.message.ServerStatusMessage'
JOBLOG_MESSAGE = 'fi.csc.microarray.messaging.message.JobLogMessage'


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


def populate_msg_body(msg_type, as_id, job_id):
    if msg_type == 'choose':
        return {"map": {"entry": [
                {"string": ["command", "choose"]},
                {"string": ["named-parameter-value1", as_id]},
                {"string": ["named-parameter-key1", "as-id"]},
                {"string": ["named-parameter-value0", job_id]},
                {"string": ["named-parameter-key0", "job-id"]}
                ]}}
    elif msg_type == 'offer':
        return {"map": {"entry": [
                {"string": ["command", "offer"]},
                {"string": ["named-parameter-value1", as_id]},
                {"string": ["named-parameter-key1", "as-id"]},
                {"string": ["named-parameter-value0", job_id]},
                {"string": ["named-parameter-key0", "job-id"]}
                ]}}
    else:
        assert RuntimeError("unknown message type")


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


def populate_comp_status_body(command):
    return '{"map": {"entry": {"string": ["command", "%s"]}}}' % command


def populate_job_running_body(job_id):
    # AMQ Stomp message transformation assumes dict fields in certain order
    # which is why this message is not serialized through json library
    return ('{"map": {"entry": [{"string": ["jobId", "%s"]},'
            '{"string": "heartbeat", "boolean": "false"},'
            '{"string": ["exitState", "RUNNING"]}]}}' % job_id)


def populate_job_result_body(job_id, exit_state='ERROR'):
    # AMQ Stomp message transformation assumes dict fields in certain order
    # which is why this message is not serialized through json library
    return ('{"map": {"entry": [{"string": "errorMessage","null": ""},'
            '{"string":"heartbeat","boolean":"false"},'
            '{"string": ["jobId", "%s"]},'
            '{"string": ["exitState", "%s"]}]}}' % (job_id, exit_state))


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

    >>> d1 = {"map":{"entry":[{"string":["command","choose"]},{"string":["named-parameter-value1","8a98c0c3-5560-41df-8b86-67435b25d565"]},{"string":["named-parameter-key1","as-id"]},{"string":["named-parameter-value0","bab581a8-0d74-47e1-a753-18b4ad032153"]},{"string":["named-parameter-key0","job-id"]}]}}
    >>> parse_msg_body(d1)
    {'job-id': 'bab581a8-0d74-47e1-a753-18b4ad032153', 'as-id': '8a98c0c3-5560-41df-8b86-67435b25d565', 'command': 'choose'}
    >>> d2 = {"map": {"entry": [{"string": ["payload_input", "214a6078-9ee9-476c-932d-d073fc7658fd"]}, {"string": ["analysisID", "test-data-in.R"]}, {"string": ["jobID", "bab581a8-0d74-47e1-a753-18b4ad032153"]}]}}
    >>> parse_msg_body(d2)
    {'analysisID': 'test-data-in.R', 'payload_input': '214a6078-9ee9-476c-932d-d073fc7658fd', 'jobID': 'bab581a8-0d74-47e1-a753-18b4ad032153'}
    >>> d3 = {u'map': {u'entry': [{u'null': u'', u'string': u'errorMessage'}, {u'string': [u'jobId', u'7f03370e-714d-450d-8707-4cf4b478fadf']}, {u'string': [u'outputText', u'''data-input-test.txt")''']}, {u'string': [u'sourceCode', u'chipster.tools.path = "aa")']}, {u'string': [u'stateDetail', u'transferring output data']},{u'string': [u'exitState', u'RUNNING']}]}}
    >>> parse_msg_body(d3)
    {u'outputText': u'data-input-test.txt")', u'jobId': u'7f03370e-714d-450d-8707-4cf4b478fadf', u'sourceCode': u'chipster.tools.path = "aa")', u'exitState': u'RUNNING', u'stateDetail': u'transferring output data'}
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


if __name__ == '__main__':
    import doctest
    doctest.testmod()
