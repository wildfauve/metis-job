from bevy import inject, dependency

from metis_job import JobConfig


@inject
def configuration(cfg: JobConfig = dependency()):
    return cfg

