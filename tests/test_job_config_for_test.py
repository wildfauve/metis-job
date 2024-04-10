import pytest
from bevy import inject, dependency

import metis_job

from .shared import *



def it_create_a_job_config():
    cfg = metis_job.JobConfig(catalogue="my_domain",
                              data_product="my_data_product",
                              service_name="my_service")

    assert cfg.catalogue == "my_domain"


def it_normalises_names_to_snake_case():
    cfg = metis_job.JobConfig(catalogue="myDomain",
                              data_product="MyDataProduct",
                              service_name="myService")

    assert cfg.catalogue == "my_domain"


# Helpers
