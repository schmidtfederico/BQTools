import os
import logging
import multiprocessing
from multiprocessing.pool import ThreadPool
from datetime import datetime
import pytz
import argparse

from google.cloud.bigquery import TimePartitioning, LoadJobConfig

from core.bq import format_bytes, BQTable
from core.cmd import setup_logging, parse_table_name, load_api_key

logging.basicConfig(level=logging.INFO)

load_api_key()

from google.cloud import bigquery

parser = argparse.ArgumentParser(description='BigQuery table audit')

parser.add_argument('-o', '--output_table', metavar='output_table', dest='output_table', type=str, default=None,
                    help='the output table full name (e.g. project:Dataset.TableAudit)')

parser.add_argument('-f', '--output_file', metavar='output_file', dest='output_file', type=str,
                    default='tables_audit.csv', help='the output file name (e.g. output.csv)')

parser.add_argument('--projects', metavar='projects', dest='projects', type=str, default=None, required=False,
                    help='the comma-separated list of projects to audit (default: all)')

parser.add_argument('--datasets', metavar='datasets', dest='datasets', type=str, default=None, required=False,
                    help='the comma-separated list of datasets to audit (default: all)')

parser.add_argument('--excluded_projects', metavar='excluded_projects', dest='excluded_projects', type=str,
                    default=None, required=False, help='the comma-separated list of projects to ignore (default: None)')

parser.add_argument('--excluded_datasets', metavar='excluded_datasets', dest='excluded_datasets', type=str,
                    default=None, required=False, help='the comma-separated list of projects to ignore (default: None)')

parser.add_argument('-w', '--workers', dest='n_workers', type=int, default=10)
parser.add_argument('-v', '--verbose', dest='verbose', const=True, action='store_const', default=False)


if __name__ == '__main__':
    args = parser.parse_args()
else:
    args = parser.parse_args(args=[])

setup_logging(args.verbose)

bq_client = bigquery.Client()

out_file = args.output_file
n_workers = args.n_workers

if n_workers <= 0:
    raise RuntimeError('Number of workers should be a positive integer, received: %d' % n_workers)

worker_clients = [bigquery.Client() for i in range(n_workers)]
worker_pool = ThreadPool(n_workers)
write_lock = multiprocessing.Lock()

columns = ['project_id', 'project_name', 'dataset_id', 'table_name', 'size', 'modified_time', 'num_rows',
           'storage_cost', 'bytes_active', 'bytes_lts', 'buffer_size', 'buffer_rows', 'buffer_age',
           'partitioning_type', 'partitioning_field', 'require_partition_filter', 'clustering_fields', 'timestamp']


column_type = {'project_id': 'STRING', 'project_name': 'STRING', 'dataset_id': 'STRING', 'table_name': 'STRING',
               'size': 'STRING', 'modified_time': 'TIMESTAMP', 'num_rows': 'INTEGER', 'storage_cost': 'FLOAT',
               'bytes_active': 'INTEGER', 'bytes_lts': 'INTEGER', 'buffer_size': 'INTEGER',
               'buffer_rows': 'INTEGER', 'buffer_age': 'INTEGER', 'partitioning_type': 'STRING',
               'partitioning_field': 'STRING', 'require_partition_filter': 'BOOLEAN', 'clustering_fields': 'STRING',
               'timestamp': 'TIMESTAMP'}

column_description = {'storage_cost': 'Estimated monthly storage cost',
                      'bytes_active': 'Number of bytes considered active storage',
                      'bytes_lts': 'Long Term Storage byte count',
                      'buffer_size': 'Streaming buffer size in bytes',
                      'buffer_age': 'Buffer oldest entry time, in seconds elapsed until timestamp field'}

schema = [bigquery.SchemaField(name=f,
                               field_type=column_type[f],
                               description=column_description[f] if f in column_description else None)
          for f in columns]

output_table = None

if args.output_table is not None:
    (target_project, target_dataset, target_name) = parse_table_name(args.output_table, name='output_table')

    output_table = BQTable(target_project, target_dataset, target_name)

    if not output_table.exists():
        output_table.create(schema, partitioning=TimePartitioning(field='timestamp'))
    else:
        table_fields = {f.name: f.field_type for f in output_table.schema}

        schema_incompatibilities = []
        for c in columns:
            if c not in table_fields:
                schema_incompatibilities.append('Missing field %s in output table' % c)
            else:
                if column_type[c] != table_fields[c]:
                    schema_incompatibilities.append('Field %s should be type %s, but found type %s '
                                                    'in output table' % (c, column_type[c], table_fields[c]))

        if len(schema_incompatibilities) > 0:
            raise RuntimeError('Schema of output table is incompatible with the required schema. Incompatibilities '
                               'found: %s\n\t *' % ('\n\t *'.join(schema_incompatibilities)))

    out_file = '_tmp_table_audit.csv'


def process_table(t):
    worker_client = worker_clients[hash(t.table_id) % n_workers]

    if t.table_type != 'TABLE':
        return None

    table = worker_client.get_table(t.reference)

    utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)

    table_api_repr = table.to_api_repr()

    if 'numLongTermBytes' not in table_api_repr:
        logging.warn('numLongTermBytes not found for table %s' % (dataset.full_dataset_id + '.' + table.table_id))
        return None

    lts_gbytes = int(table_api_repr['numLongTermBytes']) / (1024 * 1024 * 1024.)
    active_gbytes = (int(table_api_repr['numBytes']) / (1024 * 1024 * 1024.)) - lts_gbytes

    buffer_size = None
    buffer_age = None
    buffer_rows = None

    if 'streamingBuffer' in table_api_repr:
        try:
            buffer_size = int(table_api_repr['streamingBuffer']['estimatedBytes'])
            buffer_rows = int(table_api_repr['streamingBuffer']['estimatedRows'])

            buffer_age = (utc_now - table.streaming_buffer.oldest_entry_time).seconds
        except KeyError:
            logging.warn('Streaming buffer stats not found for table %s' % (dataset.full_dataset_id + '.' +
                                                                            table.table_id))

    partitioning_type = None
    partitioning_field = None
    require_partition_filter = None

    if 'timePartitioning' in table_api_repr:
        partitioning_info = table_api_repr['timePartitioning']
        partitioning_type = partitioning_info['type'] if 'type' in partitioning_info else None
        partitioning_field = partitioning_info['field'] if 'field' in partitioning_info else None
        require_partition_filter = partitioning_info['requirePartitionFilter'] if 'requirePartitionFilter' in partitioning_info else None

    clustering_fields = None

    if 'clustering' in table_api_repr:
        if 'fields' in table_api_repr['clustering']:
            clustering_fields = ', '.join(table_api_repr['clustering']['fields'])

    return {
        'table_name': table.table_id,
        'size': format_bytes(table.num_bytes),
        'modified_time': table.modified.strftime('%Y-%m-%d %H:%M:%S'),
        'num_rows': table.num_rows,
        'storage_cost': active_gbytes * 0.02 + lts_gbytes * 0.01,
        'bytes_active': int(active_gbytes * (1024 * 1024 * 1024)),
        'bytes_lts': int(lts_gbytes * (1024 * 1024 * 1024)),
        'buffer_size': buffer_size,
        'buffer_rows': buffer_rows,
        'buffer_age': buffer_age,
        'partitioning_type': partitioning_type,
        'partitioning_field': partitioning_field,
        'require_partition_filter': require_partition_filter,
        'clustering_fields': '"%s"' % clustering_fields,
        'timestamp': utc_now.strftime('%Y-%m-%d %H:%M:%S')
    }

with open(out_file, 'w') as out_f:
    # Write CSV header.
    out_f.writelines(','.join(columns) + '\n')
    out_f.flush()

    for project_idx, project in enumerate(bq_client.list_projects()):
        print project.friendly_name

        for idx, dataset in enumerate(bq_client.list_datasets(project.project_id)):
            print '\t%02d:%03d: %s' % (project_idx+1, idx+1, dataset.full_dataset_id)

            rows_buffer = []
            dataset_info = {
                'project_id': project.project_id,
                'project_name': project.friendly_name,
                'dataset_id': dataset.dataset_id
            }

            for table_info in worker_pool.imap_unordered(process_table, bq_client.list_tables(dataset.reference)):
                if table_info is not None:
                    # Add dataset and project info to dictionary.
                    table_info.update(dataset_info)

                    out_f.writelines(
                        '{project_id},{project_name},{dataset_id},{table_name},{size},{modified_time},'
                        '{num_rows},{storage_cost:.09f},{bytes_active},{bytes_lts},{buffer_size},'
                        '{buffer_rows},{buffer_age},{partitioning_type},{partitioning_field},'
                        '{require_partition_filter},{clustering_fields},{timestamp}\n'.format(**table_info)
                    )

            out_f.flush()


if output_table is not None:
    with open(out_file, 'rb') as load_file:
        config = LoadJobConfig()
        config.write_disposition = 'WRITE_APPEND'
        config.skip_leading_rows = 1
        config.null_marker = 'None'
        config.quote_character = '"'
        logging.info('Loading result into BQ table %s' % output_table.get_full_name())
        load_job = bq_client.load_table_from_file(load_file, output_table.reference, job_config=config)
        try:
            load_job.result()
        except Exception, ex:
            logging.error('Failed to load data into table. Errors: %s' % '\n'.join(load_job.errors))
            raise RuntimeError('Failed to load data into table')

    os.remove(out_file)
