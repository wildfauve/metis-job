import re
from dataclasses import dataclass, field
from enum import Enum

from metis_job.util import error

normalise_pattern = pattern = re.compile(r'(?<!^)(?=[A-Z])')

def normalise(token):
    if not token:
        return token
    return normalise_pattern.sub('_', token).lower()

class JobMode(Enum):
    SPARK = "spark"
    UNITY = "unity"

@dataclass
class JobConfig:
    catalogue: str
    data_product: str
    service_name: str
    job_mode: JobMode = field(default_factory=lambda: JobMode.UNITY)

    def __post_init__(self):
        self.catalogue = normalise(self.catalogue)
        self.service_name = normalise(self.service_name)
        self.data_product = normalise(self.data_product)


