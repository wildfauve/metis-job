from metis_job import JobConfig


def job_configuration():
    cfg = JobConfig(data_product="dp1", catalogue="domain", service_name="service1")
    return cfg

