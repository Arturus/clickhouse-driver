from .progress import Progress
import numpy as np
import pandas as pd
import itertools
from pandas.api.types import union_categoricals

class QueryResult(object):
    """
    Stores query result from multiple blocks.
    """

    def __init__(
            self, packet_generator,
            with_column_types=False, columnar=False):
        self.packet_generator = packet_generator
        self.with_column_types = with_column_types

        self.data = []
        self.columns_with_types = []
        self.columnar = columnar

        super(QueryResult, self).__init__()

    def store(self, packet):
        block = getattr(packet, 'block', None)
        if block is None:
            return

        # Header block contains no rows. Pick columns from it.
        if block.rows:
            if self.columnar:
                columns = block.get_columns()
                self.data.append(columns)
            else:
                self.data.extend(block.get_rows())

        elif not self.columns_with_types:
            self.columns_with_types = block.columns_with_types

    def get_result(self):
        """
        :return: Stored query result.
        """

        for packet in self.packet_generator:
            self.store(packet)

        if self.columnar:
            # Flatten the data (list of column chunk blocks)
            data = []
            # Transpose to a list of columns, each column is list of chunks
            for chunks in zip(*self.data):
                # Concatenate chunks for each column
                if isinstance(chunks[0], np.ndarray):
                    column = np.concatenate(chunks)
                elif isinstance(chunks[0], pd.Categorical):
                    column = union_categoricals(chunks)
                else:
                    column = tuple(itertools.chain.from_iterable(chunks))
                data.append(column)
        else:
            data = self.data
        if self.with_column_types:
            return data, self.columns_with_types
        else:
            return data


class ProgressQueryResult(QueryResult):
    """
    Stores query result and progress information from multiple blocks.
    Provides iteration over query progress.
    """

    def __init__(
            self, packet_generator,
            with_column_types=False, columnar=False):
        self.progress_totals = Progress()

        super(ProgressQueryResult, self).__init__(
            packet_generator, with_column_types, columnar
        )

    def store_progress(self, progress_packet):
        self.progress_totals.rows += progress_packet.rows
        self.progress_totals.bytes += progress_packet.bytes
        self.progress_totals.total_rows += progress_packet.total_rows
        return self.progress_totals.rows, self.progress_totals.total_rows

    def __iter__(self):
        return self

    def next(self):
        while True:
            packet = next(self.packet_generator)
            progress_packet = getattr(packet, 'progress', None)
            if progress_packet:
                return self.store_progress(progress_packet)
            else:
                self.store(packet)

    # For Python 3.
    __next__ = next

    def get_result(self):
        # Read all progress packets.
        for _ in self:
            pass

        return super(ProgressQueryResult, self).get_result()


class IterQueryResult(object):
    """
    Provides iteration over returned data by chunks (streaming by chunks).
    """

    def __init__(
            self, packet_generator,
            with_column_types=False):
        self.packet_generator = packet_generator
        self.with_column_types = with_column_types

        self.first_block = True
        super(IterQueryResult, self).__init__()

    def __iter__(self):
        return self

    def next(self):
        packet = next(self.packet_generator)
        block = getattr(packet, 'block', None)
        if block is None:
            return []

        if self.first_block and self.with_column_types:
            self.first_block = False
            rv = [block.columns_with_types]
            rv.extend(block.get_rows())
            return rv
        else:
            return block.get_rows()

    # For Python 3.
    __next__ = next


class QueryInfo(object):
    def __init__(self):
        self.profile_info = None
        self.progress = None
        self.elapsed = None

    def store_profile(self, packet):
        self.profile_info = packet.profile_info

    def store_progress(self, packet):
        progress = packet.progress
        if progress.bytes == 0 and self.progress and self.progress.bytes != 0:
            return
        self.progress = progress

    def store_elapsed(self, elapsed):
        self.elapsed = elapsed
