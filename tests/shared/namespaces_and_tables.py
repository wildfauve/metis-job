import pytest
import metis_job

from . import *


@pytest.fixture
def dataproduct1_ns():
    job_config = metis_job.JobConfig(domain_name="testDomain",
                                     data_product="dp1",
                                     service_name="test-job",
                                     job_mode=metis_job.JobMode.SPARK)

    namespace = metis_job.NameSpace(session=spark_test_session.spark_session(),
                                    job_config=job_config)

    yield namespace

    namespace.drop_namespace()

