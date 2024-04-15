from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from pyspark.sql import DataFrame

from metis_job.util import dataklass, error

from . import batch_runner


@dataclass
class BatchValue(dataklass.BaseValue):
    """
    """
    config: batch_runner.Batcher
    input_df: DataFrame | None = None
    transformed_df: DataFrame | None = None
    run_ctx: Any | None = None
    error: error.BaseError | None = None
