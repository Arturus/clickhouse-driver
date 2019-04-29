from contextlib import contextmanager
import subprocess
from unittest import TestCase

from clickhouse_driver.client import Client
from clickhouse_driver.util import compat
from tests import log


if compat.PY3:
    import configparser
else:
    import ConfigParser as configparser


file_config = configparser.ConfigParser()
file_config.read(['setup.cfg'])


log.configure(file_config.get('log', 'level'))


class BaseTestCase(TestCase):
    host = file_config.get('db', 'host')
    port = file_config.getint('db', 'port')
    database = file_config.get('db', 'database')
    user = file_config.get('db', 'user')
    password = file_config.get('db', 'password')

    client = None
    client_kwargs = None
    cli_client_kwargs = None

    @classmethod
    def emit_cli(cls, statement, database=None, encoding='utf-8', **kwargs):
        if database is None:
            database = cls.database

        args = [
            'clickhouse-client',
            '--database', database,
            '--host', cls.host,
            '--port', str(cls.port),
            '--query', str(statement)
        ]

        for key, value in kwargs.items():
            args.extend(['--' + key, str(value)])

        process = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        output = process.communicate()
        out, err = output

        if err:
            raise RuntimeError(
                'Error during communication. {}'.format(err)
            )

        return out.decode(encoding)

    def _create_client(self, **kwargs):
        return Client(
            self.host, self.port, self.database, self.user, self.password,
            **kwargs
        )

    @contextmanager
    def created_client(self, **kwargs):
        client = self._create_client(**kwargs)

        try:
            yield client
        finally:
            client.disconnect()

    @classmethod
    def setUpClass(cls):
        cls.emit_cli(
            'DROP DATABASE IF EXISTS {}'.format(cls.database), 'default'
        )
        cls.emit_cli('CREATE DATABASE {}'.format(cls.database), 'default')

        super(BaseTestCase, cls).setUpClass()

    def setUp(self):
        super(BaseTestCase, self).setUp()
        if callable(self.client_kwargs):
            version_str = self.emit_cli('SELECT version()')
            version = tuple(int(x) for x in version_str.strip().split('.'))
            client_kwargs = self.client_kwargs(version)
        else:
            client_kwargs = self.client_kwargs
        client_kwargs = client_kwargs or {}
        self.client = self._create_client(**client_kwargs)

    def tearDown(self):
        self.client.disconnect()
        super(BaseTestCase, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        cls.emit_cli('DROP DATABASE {}'.format(cls.database))
        super(BaseTestCase, cls).tearDownClass()

    @contextmanager
    def create_table(self, columns, **kwargs):
        self.emit_cli(
            'CREATE TABLE test ({}) ''ENGINE = Memory'.format(columns),
            **kwargs
        )
        try:
            yield
        except Exception:
            raise
        finally:
            self.emit_cli('DROP TABLE test')
