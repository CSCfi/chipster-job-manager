import pytest

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from jobmanager.models import Base, JobNotFound
from jobmanager.models import add_job, get_job, get_jobs, update_job_as


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
        add_job(self.session, "abc43", "Another analysis Job", "{}", "session_id")
        jobs = [x for x in get_jobs(self.session)]
        assert len(jobs) == 2

    def test_submit_job_to_as(self):
        add_job(self.session, "abc42", "analysis job", "{}", "session_id")
        update_job_as(self.session, "abc42", "analysis_server_1")
        jobs = [x for x in get_jobs(self.session)]
        assert len(jobs) == 1
        job = jobs[0]
        assert job.job_id == 'abc42'
        assert job.selected_as == 'analysis_server_1'

    def test_get_nonexisting_job(self):
        with pytest.raises(JobNotFound):
            get_job(self.session, "abc")

    def test_submit_nonexisting_job_to_as(self):
        with pytest.raises(JobNotFound):
            update_job_as(self.session, "abc42", "analysis_server_1")
