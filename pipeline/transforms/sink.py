from apache_beam import PTransform
from apache_beam import Map
from apache_beam import io
from pipeline.schemas.output import build as output_schema

class Sink(PTransform):
    def __init__(self, table=None, write_disposition=None):
        self.table = table
        self.write_disposition = write_disposition

    def expand(self, xs):
        big_query_sink = io.gcp.bigquery.BigQuerySink(
            table=self.table,
            write_disposition=self.write_disposition,
            schema=output_schema(),
        )
        return (
            xs
            | io.Write(big_query_sink)
        )
