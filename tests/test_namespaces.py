import pytest
from bevy import inject, dependency

import metis_job
from metis_job.util import error

from .shared import *

def it_creates_the_namespace(di_initialise_spark,
                             dataproduct1_ns):
    assert dataproduct1_ns.namespace_exists()


# Helpers


