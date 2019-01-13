import numpy as np
import pandas as pd
from .base import Column


class NumpyColumn(Column):
    dtype = None

    def read_items(self, n_items, buf):
        data = buf.read(n_items * self.dtype.itemsize)
        return np.frombuffer(data, self.dtype, n_items)

    def write_items(self, items, buf):
        raise RuntimeError("Write is not implemented")
        #raise NotImplementedError("Write is not implemented")


class NumpyDateTimeColumn(NumpyColumn):
    dtype = np.dtype(np.uint32)
    py_types = (int,)
    format = 'I'

    def __init__(self, timezone=None, **kwargs):
        super().__init__(**kwargs)
        self.timezone = timezone

    def read_items(self, n_items, buf):
        data = super().read_items(n_items, buf)
        dt = data.astype('datetime64[s]')
        if self.timezone:
            ts = pd.to_datetime(dt, utc=True)
            dt = ts.tz_convert(self.timezone).tz_localize(None).values
        return dt


class NumpyStringColumn(NumpyColumn):
    dtype = np.dtype('object')

    def read_items(self, n_items, buf):
        return np.array(buf.read_strings(n_items, decode=True))


class NumpyInt8Column(NumpyColumn):
    dtype = np.dtype(np.int8)
    ch_type = 'Int8'


class NumpyUInt8Column(NumpyColumn):
    dtype = np.dtype(np.uint8)
    ch_type = 'UInt8'


class NumpyInt16Column(NumpyColumn):
    dtype = np.dtype(np.int16)
    ch_type = 'Int16'


class NumpyUInt16Column(NumpyColumn):
    dtype = np.dtype(np.uint16)
    ch_type = 'UInt16'


class NumpyInt32Column(NumpyColumn):
    dtype = np.dtype(np.int32)
    ch_type = 'Int32'


class NumpyUInt32Column(NumpyColumn):
    dtype = np.dtype(np.uint32)
    ch_type = 'UInt32'


class NumpyInt64Column(NumpyColumn):
    dtype = np.dtype(np.int64)
    ch_type = 'Int64'


class NumpyUInt64Column(NumpyColumn):
    dtype = np.dtype(np.uint64)
    ch_type = 'UInt64'


class NumpyFloat32Column(NumpyColumn):
    dtype = np.dtype(np.float32)
    ch_type = 'Float32'


class NumpyFloat64Column(NumpyColumn):
    dtype = np.dtype(np.float64)
    ch_type = 'Float64'


def create_numpy_dt_column(spec, column_options):
    context = column_options['context']

    tz_name = None

    # Use column's timezone if it's specified.
    if spec[-1] == ')':
        tz_name = spec[10:-2]
    else:
        if not context.settings.get('use_client_time_zone', False):
            tz_name = context.server_info.timezone

    return NumpyDateTimeColumn(tz_name)
