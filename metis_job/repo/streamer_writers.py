from __future__ import annotations
import metis_job
from metis_job.repo import spark_util


class SparkStreamingTableWriter:
    default_stream_trigger_condition = {'availableNow': True}

    def write_stream(self,
                     streaming_df,
                     stream_reader):
        streaming_query = (streaming_df
                           .writeStream
                           .option('checkpointLocation', stream_reader.checkpoint_location)
                           .trigger(**self.__class__.default_stream_trigger_condition)
                           .toTable(stream_reader.stream_to_table_name))
        streaming_query.awaitTermination()
        return streaming_query


class DeltaStreamingTableWriter:
    format = "delta"
    default_stream_trigger_condition = {'availableNow': True}

    def write_stream(self,
                     streaming_df,
                     stream_reader):
        return self._write_stream_append_only(streaming_df, stream_reader)

    def _write_stream_append_only(self,
                                  streaming_df,
                                  stream_reader,
                                  options: list[spark_util.SparkOption] = None):
        opts = {**spark_util.SparkOption.function_based_options(options if options else []),
                **{'checkpointLocation': stream_reader.checkpoint_location}}
        streaming_query = (streaming_df.writeStream
                           .format(self.__class__.format)
                           .outputMode("append")
                           .options(**opts)
                           .trigger(**self.__class__.default_stream_trigger_condition)
                           .toTable(stream_reader.to_table_name()))
        streaming_query.awaitTermination()
        return streaming_query
