import numpy as np
import pandas as pd
from clickhouse_driver.reader import read_binary_uint64
from .base import Column
from .lowcardinalitycolumn import LowCardinalityColumn


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


class NumpyLowCardinalityColumn(LowCardinalityColumn):
    int_types = {
        0: NumpyUInt8Column,
        1: NumpyUInt16Column,
        2: NumpyUInt32Column,
        3: NumpyUInt64Column
    }

    def __init__(self, nested_column, **kwargs):
        super().__init__(nested_column, **kwargs)

    def _read_data(self, n_items, buf, nulls_map=None):
        if not n_items:
            return tuple()

        serialization_type = read_binary_uint64(buf)

        # Lowest byte contains info about key type.
        key_type = serialization_type & 0xf
        keys_column = self.int_types[key_type]()

        nullable = self.nested_column.nullable
        # Prevent null map reading. Reset nested column nullable flag.
        self.nested_column.nullable = False

        index_size = read_binary_uint64(buf)
        index = self.nested_column.read_data(index_size, buf)

        read_binary_uint64(buf)  # number of keys
        keys = keys_column.read_data(n_items, buf)

        if nullable:
            # Shift all codes by one ("No value" code is -1 for pandas categorical) and drop corresponding first index
            # this is analog of original operation: index = (None, ) + index[1:]
            keys = keys - 1
            index = index[1:]
        result = pd.Categorical.from_codes(keys, index)
        return result


def create_numpy_low_cardinality_column(spec, column_by_spec_getter):
    inner = spec[15:-1]
    nested = column_by_spec_getter(inner)
    return NumpyLowCardinalityColumn(nested)


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
