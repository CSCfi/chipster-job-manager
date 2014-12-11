===================
Chipster JobManager
===================

Chipster JobManager provides capabilities to move the Chipster job management
responsibility away from clients to remote Chipster JobManager service. When
JobManager is in use, the Chipster clients can start a job execution, save the
session state locally and disconnect. Job is then executed on Chipster analysis
servers and managed by the JobManager, which saves the results upon completion
and reschedules the job in case the execution fails.

To use JobManager with Chipster the message routing configuration must be
changed so that JobManager sits in between the client and analysis servers. 

The JobManager uses Stomp, so following entry must be added to
``transportConnectors`` section in ``chipster/activemq/conf/activemq.xml``:

    ``<transportConnector name="stomp" uri="stomp://<server_ip>:61613"/>``

The port number 61613 is the default for stomp protocol.



To learn more about Chipster refer to:

    http://chipster.csc.fi/


==========
Developing
==========

Run tests (requires tox)
    ``tox .``

Run coverage for tests (requires nose, pytest, coverage)
    ``nosetests --cover-package=jobmanager --with-coverage``
