from jobmanager.utils import parse_msg_body


class TestUtils(object):
    def setUp(self):
        pass

    def test_parse_msg_body(self):
        d1 = {"map": {"entry": [{"string": ["command", "choose"]}, {"string": ["named-parameter-value1", "8a98c0c3-5560-41df-8b86-67435b25d565"]}, {"string": ["named-parameter-key1", "as-id"]}, {"string": ["named-parameter-value0", "bab581a8-0d74-47e1-a753-18b4ad032153"]}, {"string": ["named-parameter-key0", "job-id"]}]}}
        r1 = parse_msg_body(d1)
        c1 = {'job-id': 'bab581a8-0d74-47e1-a753-18b4ad032153', 'as-id': '8a98c0c3-5560-41df-8b86-67435b25d565', 'command': 'choose'}
        assert r1 == c1
        d2 = {"map": {"entry": [{"string": ["payload_input", "214a6078-9ee9-476c-932d-d073fc7658fd"]}, {"string": ["analysisID", "test-data-in.R"]}, {"string": ["jobID", "bab581a8-0d74-47e1-a753-18b4ad032153"]}]}}
        r2 = parse_msg_body(d2)
        c2 = {'analysisID': 'test-data-in.R', 'payload_input': '214a6078-9ee9-476c-932d-d073fc7658fd', 'jobID': 'bab581a8-0d74-47e1-a753-18b4ad032153'}
        assert r2 == c2
        d3 = {u'map': {u'entry': [{u'null': u'', u'string': u'errorMessage'}, {u'string': [u'jobId', u'7f03370e-714d-450d-8707-4cf4b478fadf']}, {u'string': [u'outputText', u'''data-input-test.txt")''']}, {u'string': [u'sourceCode', u'chipster.tools.path = "aa")']}, {u'string': [u'stateDetail', u'transferring output data']}, {u'string': [u'exitState', u'RUNNING']}]}}
        r3 = parse_msg_body(d3)
        c3 = {u'outputText': u'data-input-test.txt")', u'jobId': u'7f03370e-714d-450d-8707-4cf4b478fadf', u'sourceCode': u'chipster.tools.path = "aa")', u'exitState': u'RUNNING', u'stateDetail': u'transferring output data'}
        assert r3 == c3
        d4 = {u'map': {u'entry': [{u'string': [u'command', u'cancel']}, {u'string': [u'named-parameter-value0', u'96f7ab23-2fa8-4a81-8097-de99af1c74fa']}, {u'string': [u'named-parameter-key0', u'job-id']}]}}
        r4 = parse_msg_body(d4)
        c4 = {u'job-id': u'96f7ab23-2fa8-4a81-8097-de99af1c74fa', u'command': u'cancel'}
        assert r4 == c4
