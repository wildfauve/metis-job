import pytest
from bevy import inject, dependency

import metis_job

from .shared import *



def it_create_a_job_config():
    cfg = metis_job.JobConfig(domain_name="my_domain",
                              data_product="my_data_product",
                              service_name="my_service")

    assert cfg.domain_name == "my_domain"


def it_normalises_names_to_snake_case():
    cfg = metis_job.JobConfig(domain_name="myDomain",
                              data_product="MyDataProduct",
                              service_name="myService")

    assert cfg.domain_name == "my_domain"


def it_loads_the_job_config_into_di():
    cfg = configuration()
    ns = namespace_for_test()

    assert cfg.domain_name == "price_rating"

# Helpers
