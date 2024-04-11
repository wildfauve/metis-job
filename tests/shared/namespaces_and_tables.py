import pytest
import metis_job
from pyspark.sql import types as T

from . import *

my_table_schema = T.StructType(
    [
        T.StructField('id', T.StringType(), True),
        T.StructField('isDeleted', T.StringType(), True),
        T.StructField('name', T.StringType(), True),
        T.StructField('pythons',
                      T.ArrayType(T.StructType([T.StructField('id', T.StringType(), True)]), True),
                      True),
        T.StructField('season', T.StringType(), True),
        T.StructField('onStream', T.StringType(), False)
    ])


@pytest.fixture
def dataproduct1_ns():
    job_config = metis_job.JobConfig(catalogue="testDomain",
                                     data_product="dp1",
                                     service_name="test-job",
                                     job_mode=metis_job.JobMode.SPARK)

    namespace = metis_job.NameSpace(session=spark_test_session.spark_session(),
                                    job_config=job_config)

    yield namespace

    namespace.drop_namespace()


def my_table_cls():
    class MyTable(metis_job.DomainTable):
        table_name = "my_table"
        temp_table_name = "_temp_my_hive_table"
        partition_columns = ("name",)
        pruning_column = 'name'

        table_properties = [
            metis_job.TableProperty(metis_job.DataAgreementType.SCHEMA_VERSION, "0.0.1", "my_namespace")
        ]

        schema = my_table_schema

        def after_append(self, result):
            self.properties.merge_table_properties()

        def identity_merge_condition(self, name_of_baseline, update_name):
            return f"{name_of_baseline}.id = {update_name}.id"

    return MyTable


def my_table2_cls():
    class MyTable2(metis_job.DomainTable):
        table_name = "my_hive_table_2"

        table_creation_protocol = metis_job.CreateManagedDeltaTable

        temp_table_name = "_temp_my_hive_table_2"

        partition_columns = ("name",)

        pruning_column = 'name'

        schema = my_table_schema

        def after_initialise(self):
            self.perform_table_creation_protocol()

        def identity_merge_condition(self, name_of_baseline, update_name):
            return f"{name_of_baseline}.id = {update_name}.id"

    return MyTable2


def my_table_with_props_cls():
    class MyTableCreatedWithProps(metis_job.DomainTable):
        table_creation_protocol = metis_job.CreateManagedDeltaTable

        table_name = "my_hive_table_created_as_managed_table"

        table_properties = [
            metis_job.TableProperty(metis_job.DataAgreementType.SCHEMA_VERSION, "0.0.1", "my_namespace"),
            metis_job.TableProperty(metis_job.DataAgreementType.PARTITION_COLUMNS, "identity", "my_namespace"),
            metis_job.TableProperty(metis_job.DataAgreementType.PRUNE_COLUMN, "identity", "my_namespace"),
            metis_job.TableProperty(metis_job.DataAgreementType.PORT, "superTable", "my_namespace"),
            metis_job.TableProperty(metis_job.DataAgreementType.UPDATE_FREQUENCY, "daily", "my_namespace"),
            metis_job.TableProperty(metis_job.DataAgreementType.DESCRIPTION, "Some description", "my_namespace"),
        ]

        schema = my_table_schema

        def after_initialise(self):
            self.perform_table_creation_protocol()
            pass

        def identity_merge_condition(self, name_of_baseline, update_name):
            return f"{name_of_baseline}.id = {update_name}.id"

    return MyTableCreatedWithProps
