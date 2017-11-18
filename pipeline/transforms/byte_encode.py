import base64
from apache_beam import PTransform
from apache_beam import GroupByKey
from apache_beam import Map
from pipe_tools.coders.jsoncoder import JSONDict
from pipeline.objects.feature import FeatureCoder

import numpy as np

class ByteEncode(PTransform):

    def byte_encode(self, feature):
        with_numeric_timestamps = FeatureCoder._encode(feature)[1:]
        as_bytes = base64.standard_b64encode(np.array(with_numeric_timestamps).tobytes())
        return JSONDict(id=feature.id, timestamp=feature.timestamp, value=as_bytes)


    def expand(self, xs):
        return (
            xs
            | Map(self.byte_encode)
        )


