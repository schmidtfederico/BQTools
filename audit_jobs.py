from __future__ import print_function

import argparse
import os
import logging

from multiprocessing.pool import ThreadPool
from datetime import datetime, timedelta

import csv

from google.api_core.exceptions import Forbidden
from google.cloud.bigquery import TimePartitioning, LoadJobConfig, QueryJobConfig

from core.bq import BQTable
from core.cmd import setup_logging, parse_table_name, load_api_key

load_api_key()

from google.cloud import bigquery, storage
import json

bq_client = bigquery.Client()


parser = argparse.ArgumentParser(description='BigQuery jobs audit')

parser.add_argument('-o', '--output_table', metavar='output_table', dest='output_table', type=str, default=None,
                    help='the output table full name (e.g. project:Dataset.JobsAudit)')

parser.add_argument('-f', '--output_file', metavar='output_file', dest='output_file', type=str,
                    default='jobs_audit.csv', help='the output file name (e.g. output.csv)')

parser.add_argument('-t', '--min_creation_time', metavar='min_creation_time', dest='min_creation_time', type=str,
                    default=None, help='the UTC timestamp since when to export jobs, with format YYYY-MM-DD_HH:MM:SS'
                                       '(default: auto - if output is a table, max creation timestamp exported from it,'
                                       'otherwise since the beginning)')

parser.add_argument('--storage_project', metavar='storage_project', dest='storage_project', type=str, default=None,
                    help='the Google Cloud Storage project from where the results will be loaded into BigQuery '
                         '(only needed when outputting to a BigQuery table). Will default to the table project if '
                         'not defined.')

parser.add_argument('--storage_bucket', metavar='storage_bucket', dest='storage_bucket', type=str,
                    help='the Google Cloud Storage bucket, inside the storage_project, from where the results will be '
                         'loaded into BigQuery (only needed when outputting to a BigQuery table)')

parser.add_argument('-b', '--bucket', metavar='bucket', dest='bucket', type=str,
                    help='the output file name (e.g. output.csv)')

parser.add_argument('-w', '--workers', dest='n_workers', type=int, default=1)
parser.add_argument('-v', '--verbose', dest='verbose', const=True, action='store_const', default=False)


def table_name(table_reference):
    return '%s:%s.%s' % (table_reference.project, table_reference.dataset_id, table_reference.table_id)


def process_job(j):
    job_values = {
        k: getattr(j, k, None) for k in job_attributes
    }

    if getattr(j, 'destination', None) and not j.destination.table_id.startswith('anon'):
        job_values['output_table'] = table_name(j.destination)
        job_values['write_disposition'] = j.write_disposition
    else:
        job_values['write_disposition'] = None

    if getattr(j, '_properties', None):
        if 'jobReference' in j._properties:
            job_values['project_id'] = j._properties['jobReference'].get('projectId')

    if j.error_result:
        job_values['error_reason'] = j.error_result['reason']
        job_values['error_message'] = j.error_result['message']

    if getattr(j, 'referenced_tables', None) and len(j.referenced_tables) > 0:
        job_values['referenced_tables'] = ','.join([table_name(t) for t in j.referenced_tables])
    else:
        job_values['referenced_tables'] = None

    if getattr(j, 'source', None):
        job_values['source'] = table_name(j.source)
        job_values['destination_uris'] = ','.join(job_values['destination_uris'])

    if job_values['query']:
        job_values['query'] = job_values['query'].encode('utf8')

    return job_values


if __name__ == '__main__':
    args = parser.parse_args()
else:
    args = parser.parse_args(args=[])

setup_logging(args.verbose)

worker_pool = ThreadPool(args.n_workers)

job_attributes = [
    'job_id', 'job_type', 'project', 'project_id', 'user_email', 'created', 'started', 'ended', 'total_bytes_processed',
    'total_bytes_billed', 'use_legacy_sql', 'error_reason', 'query', 'output_table', 'write_disposition',
    'error_message', 'referenced_tables', 'source', 'destination_uris'
]

attribute_type = ['STRING'] * 5 + ['TIMESTAMP'] * 3 + ['INTEGER'] * 2 + ['BOOLEAN'] + ['STRING'] * 8

column_type = {job_attributes[i]: attribute_type[i] for i in range(len(job_attributes))}

column_description = {}

schema = [bigquery.SchemaField(name=f,
                               field_type=column_type[f],
                               description=column_description[f] if f in column_description else None)
          for f in job_attributes]

output_table = None

if args.output_table is not None:
    (target_project, target_dataset, target_name) = parse_table_name(args.output_table, name='output_table')

    output_table = BQTable(target_project, target_dataset, target_name)

    if not output_table.exists():
        output_table.create(schema, partitioning=TimePartitioning(field='created'))
    else:
        table_fields = {f.name: f.field_type for f in output_table.schema}

        schema_incompatibilities = []
        for c in job_attributes:
            if c not in table_fields:
                schema_incompatibilities.append('Missing field %s in output table' % c)
            else:
                if column_type[c] != table_fields[c]:
                    schema_incompatibilities.append('Field %s should be type %s, but found type %s '
                                                    'in output table' % (c, column_type[c], table_fields[c]))

        if len(schema_incompatibilities) > 0:
            raise RuntimeError('Schema of output table is incompatible with the required schema. Incompatibilities '
                               'found: %s\n\t *' % ('\n\t *'.join(schema_incompatibilities)))

    out_file = '_tmp_jobs_audit_%s.csv' % datetime.now().strftime('%Y-%m-%d')
else:
    out_file = args.output_file


min_creation_time = args.min_creation_time

if min_creation_time is None:
    if output_table is not None:
        # q = QueryJobConfig()
        # q.use_legacy_sql = True
        #
        # partitions_query = output_table.get_client().query(
        #     "SELECT MAX(partition_id) AS partition_id FROM [%s$__PARTITIONS_SUMMARY__] "
        #     "WHERE partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')" % output_table.get_full_name(),
        #     job_config=q)
        #
        # partition_id = [row['partition_id'] for row in partitions_query.result()]
        # partition_id = [partition for partition in partition_id if partition is not None]
        #
        # if len(partition_id) < 1:
        #     pass
        # else:
        #     partition_date = datetime.strptime(partition_id[0], '%Y%m%d')

        # max_ts_query = 'SELECT project_id, MAX(created) AS ts FROM %s ' \
        #                'WHERE created >= "%s 00:00:00"' \
        #                'GROUP BY project_id' % (
        #     output_table.get_full_name(standard=True, quoted=True),
        #     partition_date.strftime('%Y-%m-%d')
        # )

        max_ts_query = 'SELECT project_id, MAX(created) AS ts ' \
                       'FROM %s GROUP BY project_id' % (output_table.get_full_name(standard=True, quoted=True))

        max_ts_query = output_table.get_client().query(max_ts_query)

        min_creation_time = {row['project_id']: row['ts'] for row in max_ts_query.result()}

else:
    min_creation_time = datetime.strptime(min_creation_time, '%Y-%m-%d_%H:%M:%S')

if os.path.exists(out_file):
    logging.warning('Deleted previous output file %s' % out_file)
    os.remove(out_file)

n_jobs_found = 0

with open(out_file, 'w') as f:
    writer = csv.DictWriter(f, fieldnames=job_attributes)
    writer.writeheader()

    for project in bq_client.list_projects():
        creation_filter = min_creation_time

        if isinstance(min_creation_time, dict):
            if project.project_id in min_creation_time:
                creation_filter = min_creation_time[project.project_id]
            else:
                creation_filter = None

        logging.debug('Fetching jobs since %s for project %s' % (creation_filter, project.project_id))

        jobs = bq_client.list_jobs(project=project.project_id, state_filter='done',
                                   all_users=True, min_creation_time=creation_filter)

        for job_info in worker_pool.imap_unordered(process_job, jobs):
            if job_info:
                try:
                    writer.writerow(job_info)
                    n_jobs_found += 1
                except UnicodeEncodeError:
                    logging.warning('Failed to write a job')
                    print(job_info)

    if n_jobs_found == 0:
        os.remove(out_file)
        logging.info('No new jobs found')


if output_table is not None and os.path.exists(out_file):
    # Create a load job configuration.
    job_config = LoadJobConfig()
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.skip_leading_rows = 1
    job_config.quote_character = '"'
    job_config.autodetect = True

    file_size_mb = os.stat(out_file).st_size / (1024. * 1014)

    if file_size_mb < 9:
        # Small files can be loaded directly from the Python API.
        with open(out_file, 'rb') as load_file:
            logging.info('Loading result into BQ table %s' % output_table.get_full_name())
            load_job = bq_client.load_table_from_file(load_file,
                                                      output_table.reference,
                                                      job_config=job_config)

            try:
                load_job.result()
            except Exception:
                logging.error('Failed to load data into table. Errors: \n%s' % json.dumps(load_job.errors, indent=4))
                raise RuntimeError('Failed to load data into table')
    else:
        # We need to use Google Cloud Storage and append to the table from a GCS URI.

        # Default to the table project if the storage project is not defined.
        storage_project = args.storage_project if args.storage_project is not None else output_table.project

        storage_client = storage.Client(project=storage_project)

        if args.storage_bucket is None:
            raise RuntimeError("A Google Cloud Storage bucket name should be provided with option --storage_bucket in "
                               "order to load results to table %s" % output_table.get_full_name())

        if storage_client.lookup_bucket(args.storage_bucket) is None:
            # Try creating the bucket instead of raising an exception.
            storage_bucket = storage_client.create_bucket(args.storage_bucket)
        else:
            storage_bucket = storage_client.bucket(args.storage_bucket)

        blob = storage_bucket.blob(out_file)

        if blob.exists():
            blob.delete()

        logging.info('Uploading output to G-Cloud Storage...')
        blob.upload_from_filename(out_file)
        logging.info('Done uploading!')

        blob_uri = 'gs://%s/%s' % (storage_bucket.name, blob.name)

        logging.info('Loading output into table %s' % output_table.get_full_name())
        load_job = bq_client.load_table_from_uri(blob_uri, output_table.reference, job_config=job_config)

        result = load_job.result()

        if result.errors and len(result.errors) > 0:
            for e in result.errors:
                logging.error(e)

        # Perform cleanup.
        try:
            blob.delete()
        except Forbidden:
            logging.warning('Failed to delete Google Cloud Storage blob, maybe the API key is missing permissions?')

    os.remove(out_file)

    logging.info('Done!')

