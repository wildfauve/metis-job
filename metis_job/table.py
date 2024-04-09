from __future__ import annotations
from pyspark.sql import dataframe
from delta.tables import *

from metis_job import repo
from . import namespace as ns

ReaderType = repo.DeltaTableReader


class CreateManagedDeltaTable:
    def perform(self, table: DomainTable):
        self._create(table.namespace.session,
                     table.schema,
                     table.fully_qualified_table_name(),
                     table.partition_on(),
                     table.table_property_expr())
        # table.property_manager.invalidate_property_cache()
        pass

    def _create(self,
                spark_session,
                schema,
                fully_qualified_table_name: str,
                partition_cols,
                table_prop_expr):
        result = (DeltaTable.createIfNotExists(spark_session)
                  .tableName(fully_qualified_table_name)
                  .addColumns(schema)
                  .execute())
        return result


class DomainTable:
    schema = None
    table_name = None
    partition_columns = None
    table_properties = None

    def __init__(self,
                 namespace: ns.NameSpace = None,
                 reader: ReaderType = repo.DeltaTableReader,
                 table_creation_protocol=CreateManagedDeltaTable):
        self.namespace = namespace
        self.reader = reader
        self.table_creation_protocol = table_creation_protocol

        self.after_initialise()  # callback Hook

    def read(self) -> dataframe.DataFrame:
        return self.reader().read(self, self.fully_qualified_table_name())

    def table_exists(self) -> bool:
        return self.namespace.table_exists(self.__class__.table_name)

    def fully_qualified_table_name(self):
        return self.namespace.fully_qualified_table_name(self.table_name)

    def partition_on(self):
        return self.__class__.partition_columns if hasattr(self, 'partition_columns') else tuple()

    def perform_table_creation_protocol(self):
        if not self.table_creation_protocol and not self.__class__.table_creation_protocol:
            raise repo.hive_table_can_not_be_created_no_protocol_provided()
        if self.table_creation_protocol:
            self.table_creation_protocol().perform(self)
        else:
            self.__class__.table_creation_protocol().perform(self)
        return self

    def table_property_expr(self):
        return repo.TableProperty.table_property_expression(self.asserted_table_properties())

    def asserted_table_properties(self):
        return self.__class__.table_properties if hasattr(self, 'table_properties') else None

    # Noop Callbacks
    def after_initialise(self):
        ...
