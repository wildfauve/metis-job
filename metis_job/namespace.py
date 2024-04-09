from __future__ import annotations

from typing import Protocol
from metis_job.util import logger
from metis_job.repo import sql_builder, properties
from . import config


class NamingConventionProtocol(Protocol):

    def namespace_name(self) -> str:
        """
        The database name is provide in the dbconfig section of the job config.  This function returns that name.
        :return:
        """
        ...

    def table_name(self, table_name) -> str:
        """
        This function combines the database name and the provided table name.

        Used when using hive-based operations; like drop table, or spark.table(db_table_name("t1")

        Used by:
        + HiveTableReader().read
        + HiveRepo().drop_table_by_name
        + HiveRepo().read_stream
        + HiveRepo().create
        + HiveRepo().get_table_properties
        + HiveRepo().add_to_table_properties
        + HiveRepo().remove_from_table_properties

        :param table_name:
        :return:
        """
        ...

    def namespace_path(self) -> str:
        """
        Provide the location path for a database.  Used when creating or dropping the database.
        :return:
        """
        ...

    def namespace_table_path(self, table_name: str) -> str:
        """
        The path location of the table.
        :param table_name:
        :return:
        """
        ...

    def delta_table_location(self, table_name: str) -> str:
        """
        The load location for reading a delta table using DeltaTable class.

        DeltaTable.forPath(spark, self.delta_table_location)

        Used by Hive functions:
        + DeltaTableReader().table
        + DeltaFileReader().read
        + StreamFileWriter().write


        :param table_name:
        :return:
        """
        ...

    def checkpoint_location(self, table_name) -> str:
        """
        The location of the checkpoint folder when using delta streaming.

        :param table_name:
        :return:
        """
        ...

    def delta_table_naming_correctly_configured(self) -> bool:
        """
        True if naming has been configured correctly for a delta table location, which will include possible
        consideration for the checkpoint override required in testing.
        :return:
        """


class SparkNamingConventionDomainBased(NamingConventionProtocol):
    """
    DB and Table naming convention based on the names of the domain and data product.  Uses the following properties
    from the config:

        cfg = (spark_job.JobConfig(data_product_name=my_data_product_name,
                                   domain_name=my_domain_name
              .configure_hive_db(db_name="my_db"))

    DB paths ion the cluster are absolute paths (i.e. prepended with a "/".  In test they must be relative paths.
    This is driven by the setting of job_config().running_in_test().  Therefore, when using this strategy this must
    be set for testing.
    """

    def __init__(self, job_config):
        self.config = job_config

    def namespace_name(self):
        return self.config.data_product

    def domain_name(self):
        return self.config.domain_name

    def data_product_name(self):
        return self.config.data_product_name

    def fully_qualified_name(self, table_name):
        return f"{self.namespace_name()}.{table_name}"


class NameSpace:

    def __init__(self,
                 session,
                 job_config):
        self.session = session
        self.config = job_config
        self.naming = self.determine_naming_convention()
        self.create_namespace_if_not_exists()

    def determine_naming_convention(self):
        match self.config.job_mode:
            case config.JobMode.SPARK:
                return SparkNamingConventionDomainBased(self.config)
            case _:
                breakpoint()

    #
    # DB LifeCycle Functions
    #
    def create_namespace_if_not_exists(self):
        self.session.sql(sql_builder.create_db(db_name=self.naming.namespace_name(),
                                               db_property_expression=self.property_expr()))

    def drop_namespace(self):
        self.session.sql(f"drop database IF EXISTS {self.naming.namespace_name()} CASCADE")
        return self

    def fully_qualified_table_name(self, table_name):
        return self.naming.fully_qualified_name(table_name)

    def namespace_exists(self) -> bool:
        return self.session.catalog.databaseExists(self.naming.namespace_name())

    def table_exists(self, table_name):
        return table_name in self.list_tables()

    def catalog_table_exists(self, table_name):
        return self.session.catalog.tableExists(table_name)

    def list_tables(self):
        return [table.name for table in self.session.catalog.listTables(self.naming.namespace_name())]

    def table_format(self):
        return self.config.db.table_format

    #
    # DB Property Functions
    #
    def asserted_properties(self):
        return self.__class__.db_properties if hasattr(self, 'db_properties') else None

    def property_expr(self):
        return properties.DbProperty.property_expression(self.asserted_properties())
