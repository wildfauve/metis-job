from bevy import inject, dependency
from pyspark.sql.session import SparkSession

from .shared import *
from .shared import initialiser


def test_create_spark_session():
    spark = spark_session()

    assert spark.version
