from delta import *
import pyspark

from metis_job import session


def create_session():
    return session.build_spark_session("test_spark_session",
                                       spark_delta_session,
                                       spark_session_config)


def spark_delta_session(session_name):
    return configure_spark_with_delta_pip(delta_builder(session_name)).getOrCreate()


def delta_builder(session_name):
    return (pyspark.sql.SparkSession.builder.appName(session_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))


def spark_session_config(spark: pyspark.sql.session) -> None:
    pass
