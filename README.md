# ClickHouse Python driver for Data Science
This is a modification of excellent Python driver https://github.com/mymarilyn/clickhouse-driver.
Modified driver has support for direct data loading into numpy arrays and performance-oriented enhancements.

[Blog article](https://suilin.ru/post/clickhouse_driver/) (in Russian)

## Features
Modified driver can directly load numeric columns (Float32/64, \[U\]Int8/16/32/64), LowCardinality and
DateTime columns into numpy arrays when using columnar mode (`columnar=True`).
Direct loading increases performance by 8-35 times and lowers
memory requirements by ~4 times.

Direct loading into pandas dataframe is also supported.

## Installation

`pip uninstall clickhouse-driver` (optional, if original driver is installed)

`pip install git+https://github.com/Arturus/clickhouse-driver.git`

## Usage

```python
>>> from clickhouse_driver import Client
>>>
>>> client = Client('localhost', settings=dict(numpy_columns=True))
>>>
>>> client.execute('SELECT a, b from TABLE', columnar=True)
[array([0, 3, 0, ..., 2, 0, 1], dtype=uint8),
 array([1, 0, 4, ..., 0, 0, 0], dtype=uint8)]
>>>
>>> client.query_dataframe('SELECT a, b FROM table')
          a  b
0         0  1
1         3  0
2         0  4
3         0  0
4         0  0
...      .. ..
96045192  0  0
96045193  0  0
96045194  2  0
96045195  0  0
96045196  1  0

[96045197 rows x 2 columns]
```

If numpy support is turned on (by `numpy_columns=True` setting),
 driver will load numeric and datetime
columns as numpy arrays (or pandas Categorical type for LowCardinality columns).
 For convenience, `query_dataframe()` method
 loads all columns as pandas dataframe.


## Benchmark

Query (`SELECT x1,x2,...,xn FROM table`) performance was measured on the table with
100 million records (web analytics data), engine=MergeTree.
Requests were run on local ClickHouse instance with default driver settings.

| Query            | Time, numpy | Time, standard | Speedup | Memory, numpy| Memory, standard|
| ---------------- | :---------: | :-------------: | :---------: | :------------: | :---------------: |
| 4 columns, Int8     | 0.34 s     |      5.8 s     |   ×**17**    |   0.82 Gb    |     3.3 Gb   |
| 2 columns, Int64    | 1.38 s     |      12 s      |   ×**8.7**   |   2.61 Gb    |     9.7 Gb   |
| 1 column,  DateTime | 12.1 s     |      7.1 m     |   ×**35**    |   1.16 Gb    |     4.8 Gb   |


## Limitations

* Only reading into numpy arrays is supported. Writing is only possible in `numpy_columns=False` mode.
* Numpy arrays are not used when reading nullable columns and array columns. However, the code for reading array columns is also slightly optimized and is now faster than with a regular driver.
* Also numpy is not used when reading enums, decimal and other advanced types (support may be added in the future).

Restrictions on reading do not interfere with the functioning of the driver,
just for some data types reading speeds up, and for some it works as usual.


## License

Driver is distributed under the [MIT license](http://www.opensource.org/licenses/mit-license.php).
