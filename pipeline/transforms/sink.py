from apache_beam import PTransform
from apache_beam import Map
from apache_beam import io
from pipeline.schemas.output import build as output_schema

class Sink(PTransform):
    def __init__(self, table=None, write_disposition=None):
        self.table = table
        self.write_disposition = write_disposition

    def encode_datetime(self, value):
        return value.strftime('%Y-%m-%d %H:%M:%S.%f UTC')

    def encode_datetime_fields(self, x):
        x['timestamp'] = self.encode_datetime(x['timestamp'])
        return x

    def expand(self, xs):
        big_query_sink = io.gcp.bigquery.BigQuerySink(
            table=self.table,
            write_disposition=self.write_disposition,
            schema=output_schema(),
        )

        return (
            xs
            | Map(self.encode_datetime_fields)
            | io.Write(big_query_sink)
        )
