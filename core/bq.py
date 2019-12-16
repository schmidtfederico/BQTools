import os
import sys
from distutils.util import strtobool

from google.cloud.bigquery import TableReference

key_path = os.path.normpath(os.path.join('data', 'auth', 'bq_key.json'))

if not os.path.exists(key_path):
    raise RuntimeError('Missing Google Service Account Credentials at data/auth/bq_key.json')

# Set auth key path before loading BQ lib.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path

from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import math


def format_bytes(n):
    b_units = ['bytes', 'KB', 'MB', 'GB', 'TB']
    if n > 1:
        p = min([int(math.floor(math.log(n, 2) / 10.)), len(b_units) - 1])
    else:
        return '%.0f bytes' % n
    n = float(n) / (1024 ** p)
    return '%.2f %s' % (n, b_units[p])

bq_clients = {}


class BQTable:
    def __init__(self, project, dataset, name, partition=None, client=None):
        self.project = project
        self.dataset = dataset
        self.name = name
        self.partition = partition
        self._dataset_ref = None
        self._table_ref = None
        self._table = None
        self._schema = None
        self.client = client

    @property
    def dataset_reference(self):
        if self._dataset_ref is None:
            self._dataset_ref = self.get_client().dataset(self.dataset)
        return self._dataset_ref

    @property
    def reference(self):
        if self._table_ref is None:
            full_name = self.name if self.partition is None else self.name + '$' + self.partition
            self._table_ref = self.dataset_reference.table(full_name)
        return self._table_ref

    @property
    def schema(self):
        """
        :rtype: list of :class:`~google.cloud.bigquery.schema.SchemaField`
        """
        if self._schema is None:
            self._schema = self.get_table().schema
        return self._schema

    def get_full_name(self, standard=False, quoted=False):
        name_str = '{project}.{dataset}.{table}' if standard else '{project}:{dataset}.{table}'
        if self.partition is not None:
            if standard:
                raise RuntimeError("Can't represent a table partition full name using StandardSQL")
            name_str += '${partition}'

        if quoted:
            quotes = '`%s`' if standard else '[%s]'
            name_str = quotes % name_str

        return name_str.format(project=self.project, dataset=self.dataset, table=self.name, partition=self.partition)

    def get_client(self):
        """
        :rtype :class:`~google.cloud.bigquery.Client`
        :return A BigQuery client for this table's project.
        """
        if self.client is None:
            if self.project not in bq_clients:
                bq_clients[self.project] = bigquery.Client(project=self.project)
            self.client = bq_clients[self.project]
        return self.client

    def get_dataset(self):
        return self.get_client().get_dataset(self.dataset_reference)

    def get_table(self):
        """
        :rtype: :class:`google.cloud.bigquery.table.Table`
        """
        if self._table is None:
            self._table = self.get_client().get_table(self.reference)
        return self._table

    def get_fields_map(self):
        return {f.name: f.field_type for f in self.schema}

    def exists(self):
        try:
            self.get_table()
            return True
        except NotFound:
            return False

    def delete(self, prompt=True):
        if prompt:
            sys.stdout.write('Delete table %s? [y/N]' % self.get_full_name(quoted=True))
            proceed = raw_input().lower().strip()
            if len(proceed) == 0:
                proceed = 'no'
            if not strtobool(proceed):
                return False
        client = self.get_client()
        client.delete_table(self.reference)
        return True

    def create(self, schema, partitioning=None):
        t = bigquery.Table(self.reference, schema=schema)
        if partitioning is not None:
            t.time_partitioning = partitioning
        client = self.get_client()
        self._table = client.create_table(t)
