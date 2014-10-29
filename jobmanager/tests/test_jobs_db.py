from __future__ import unicode_literals
import pytest

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from jobmanager.models import Base, JobNotFound
from jobmanager.models import (add_job, get_job, get_jobs, update_job_comp,
                               update_job_results, update_job_reply_to,
                               update_job_rescheduled, update_job_cancelled,
                               update_job_running)


class TestDB(object):
    def setUp(self):
        engine = create_engine('sqlite:///:memory:')

        def _fk_pragma_on_connect(dbapi_con, con_record):
            dbapi_con.execute('pragma foreign_keys=ON')

        event.listen(engine, 'connect', _fk_pragma_on_connect)
        Base.metadata.create_all(engine)
        self.session = sessionmaker(bind=engine)()

    def test_add_new_job(self):
        add_job(self.session, "abc42", "Analysis Job", "{}", "session_id")
        jobs = [x for x in get_jobs(self.session)]
        assert len(jobs) == 1
        job = jobs[0]
        assert job.job_id == 'abc42'
        assert job.description == 'Analysis Job'
        assert job.headers == '{}'
        assert job.session_id == 'session_id'

    def test_add_multiple_jobs(self):
        add_job(self.session, "abc42", "analysis job", "{}", "session_id")
        jobs = [x for x in get_jobs(self.session)]
        assert len(jobs) == 1
        add_job(self.session, "abc43", "analysis Job ", "{}", "session_id")
        jobs = [x for x in get_jobs(self.session)]
        assert len(jobs) == 2

    def test_submit_job_to_comp(self):
        add_job(self.session, "abc42", "analysis job", "{}", "session_id")
        update_job_comp(self.session, "abc42", "analysis_server_1")
        jobs = [x for x in get_jobs(self.session)]
        assert len(jobs) == 1
        job = jobs[0]
        assert job.job_id == 'abc42'
        assert job.comp_id == 'analysis_server_1'

    def test_get_all_jobs(self):
        add_job(self.session, "abc42", "analysis job", "{}", "session_id")
        add_job(self.session, "abc43", "analysis job", "{}", "session_id")
        update_job_results(self.session, "abc43", "results")
        jobs_active = [x for x in get_jobs(self.session)]
        jobs_all = [x for x in get_jobs(self.session, include_finished=True)]
        assert len(jobs_active) == 1
        assert len(jobs_all) == 2

    def test_get_nonexisting_job(self):
        with pytest.raises(JobNotFound):
            get_job(self.session, "abc")

    def test_submit_nonexisting_job_to_as(self):
        with pytest.raises(JobNotFound):
            update_job_comp(self.session, "abc42", "analysis_server_1")

    def test_add_replyto_to_nonexisting_job(self):
        with pytest.raises(JobNotFound):
            update_job_reply_to(self.session, "abc42", "someaddr")

    def test_job_update(self):
        add_job(self.session, "abc42", "analysis job", "{}", "session_id")
        assert not get_job(self.session, "abc42").seen
        update_job_running(self.session, "abc42")
        assert get_job(self.session, "abc42")

    def test_job_update_nonexistent(self):
        with pytest.raises(JobNotFound):
            update_job_running(self.session, "abc42")

    def test_job_presentation(self):
        add_job(self.session, "abc42", "analysis job", "{}", "session_id")
        job_str = get_job(self.session, "abc42")
        assert "%s" % job_str == "<Job:abc42>"

    def test_reschedule_nonexisting_job(self):
        with pytest.raises(JobNotFound):
            update_job_rescheduled(self.session, "abc42")

    def test_reschedule_finished_job(self):
        add_job(self.session, "abc42", "analysis job", "{}", "session_id")
        update_job_results(self.session, "abc42", "results")
        with pytest.raises(RuntimeError):
            update_job_rescheduled(self.session, "abc42")

    def test_reschedule_job(self):
        add_job(self.session, "abc42", "analysis job", "{}", "session_id")
        job = get_job(self.session, "abc42")
        assert job.retries == 0
        update_job_rescheduled(self.session, "abc42")
        assert job.retries == 1

    def test_cancel_job(self):
        add_job(self.session, "abc42", "analysis job", "{}", "session_id")
        update_job_cancelled(self.session, "abc42")
        job = get_job(self.session, "abc42")
        assert job.finished
        assert not job.results

    def test_cancel_nonexistent(self):
        with pytest.raises(JobNotFound):
            update_job_cancelled(self.session, "abc42")

    def test_cancel_completed(self):
        add_job(self.session, "abc42", "analysis job", "{}", "session_id")
        update_job_results(self.session, "abc42", "results")
        with pytest.raises(RuntimeError):
            update_job_cancelled(self.session, "abc42")
