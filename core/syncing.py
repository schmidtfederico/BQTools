from datetime import datetime, timedelta
import logging

import math
from google.cloud.bigquery import QueryJobConfig, CopyJobConfig

from core.bq import BQTable, format_bytes
from tqdm import tqdm

join_comparison = {
    'STRING': "COALESCE({source_field}, '*(nullstr)*') = COALESCE({target_field}, '*(nullstr)*')",
    'INTEGER': "COALESCE(CAST({source_field} AS STRING), '*(nullstr)*') = "
               "COALESCE(CAST({target_field} AS STRING), '*(nullstr)*')",
    'FLOAT': "COALESCE(CAST({source_field} AS STRING), '*(nullstr)*') = "
               "COALESCE(CAST({target_field} AS STRING), '*(nullstr)*')",
    'TIMESTAMP': "ABS(TIMESTAMP_DIFF(COALESCE({source_field}, TIMESTAMP_SECONDS(0)), "
                 "COALESCE({target_field}, TIMESTAMP_SECONDS(0)), MILLISECOND)) < 5",
    'BOOLEAN': "COALESCE(CAST({source_field} AS STRING), '*(nullstr)*') = "
               "COALESCE(CAST({target_field} AS STRING), '*(nullstr)*')",
    'NUMERIC': "COALESCE(CAST({source_field} AS STRING), '*(nullstr)*') = "
               "COALESCE(CAST({target_field} AS STRING), '*(nullstr)*')",
    'DATE': "COALESCE(CAST({source_field} AS STRING), '*(nullstr)*') = "
               "COALESCE(CAST({target_field} AS STRING), '*(nullstr)*')",
    'TIME': "COALESCE(CAST({source_field} AS STRING), '*(nullstr)*') = "
               "COALESCE(CAST({target_field} AS STRING), '*(nullstr)*')",
    'DATETIME': "COALESCE(CAST({source_field} AS STRING), '*(nullstr)*') = "
               "COALESCE(CAST({target_field} AS STRING), '*(nullstr)*')",
    'BYTES': "COALESCE(TO_BASE64({source_field}), '*(nullstr)*') = "
               "COALESCE(TO_BASE64({target_field}), '*(nullstr)*')"
}


class Syncing:
    def __init__(self, source_table, destination_table, pkey=None, insert_buffer_size=1000):
        """
        :param source_table: A representation of the source table.
        :type source_table: :class:`~core.bq.BQTable`

        :param destination_table: A representation of the destination table.
        :type destination_table: :class:`~core.bq.BQTable`
        """
        self.source_table = source_table
        self.destination_table = destination_table
        self.pkey = pkey
        self.insert_buffer_size = insert_buffer_size

        source_fields = set(self.source_table.get_fields_map().keys())
        destination_fields = set(self.destination_table.get_fields_map().keys())

        if not destination_fields.issubset(source_fields):
            raise RuntimeError('Destination table is not a subset of the source table (missing columns: %s)' %
                               list(destination_fields - source_fields))

        if self.pkey is not None:
            self.pkey = set(self.pkey)
        else:
            self.pkey = destination_fields

        if not self.pkey.issubset(source_fields) and self.pkey.issubset(destination_fields):
            raise RuntimeError('pkey fields should be present in both source and destination tables')

        self.sync_fields = destination_fields

    def run(self):
        decorator_time_back = datetime.utcnow() - self.destination_table.get_table().created.replace(tzinfo=None)
        decorator_time_back = decorator_time_back.total_seconds() * 2
        decorator_time_back = decorator_time_back * 1000  # In ms.

        partial_slice_query = 'SELECT [{sync_fields}] FROM [{source_table_full_name}@-{time_back:d}-]'.format(
            sync_fields='], ['.join(self.destination_table.get_fields_map().keys()),
            source_table_full_name=self.source_table.get_full_name(),
            time_back=int(math.ceil(decorator_time_back))
        )

        partial_slice_table = BQTable(project=self.destination_table.project,
                                      dataset=self.destination_table.dataset,
                                      name='%s_partial_slice' % self.source_table.name)
        run_slice_query = True

        if partial_slice_table.exists():
            logging.warning('A partial table with the last records of the source table already exists. Do you want '
                            'to delete this table and create it again or do you want to reuse it?')
            run_slice_query = partial_slice_table.delete(prompt=True)

        if run_slice_query:
            slice_query_config = QueryJobConfig()
            slice_query_config.allow_large_results = True
            slice_query_config.use_legacy_sql = True
            slice_query_config.destination = partial_slice_table.reference
            slice_query_config.write_disposition = 'WRITE_TRUNCATE'

            logging.info('Extracting the last records of the source table to a temporal table (%s)' %
                         partial_slice_table.get_full_name())

            logging.debug('Running query:\n    %s' % (partial_slice_query.replace('\n', '\n' + ' ' * 4)))

            slice_query_ref = self.destination_table.get_client().query(partial_slice_query,
                                                                        job_config=slice_query_config)

            # Wait for the query to finish running.
            slice_query_ref.result()

            logging.info('Extraction query quota usage: %s' % format_bytes(slice_query_ref.total_bytes_billed))

        diff_query = self.__build_diff_query__(partial_slice_table)

        diff_table = BQTable(project=self.destination_table.project,
                             dataset=self.destination_table.dataset,
                             name='%s_diff' % self.source_table.name)

        run_diff_query = True
        if diff_table.exists() and diff_table.get_table().num_rows > 0:
            logging.warning('A diff between the source and destination table was already found at %s, '
                            'do you want to delete this table and run the diff again or reuse this diff?' %
                            diff_table.get_full_name())
            run_diff_query = diff_table.delete(prompt=True)

        diff_query_result = None

        if run_diff_query:
            diff_query_config = QueryJobConfig()
            diff_query_config.use_legacy_sql = False
            diff_query_config.write_disposition = 'WRITE_TRUNCATE'
            diff_query_config.allow_large_results = True
            diff_query_config.destination = diff_table.reference

            logging.info('Creating a diff of %s and the last records of %s. Writing the results to %s.' % (
                self.destination_table.get_full_name(), self.source_table.get_full_name(), diff_table.get_full_name()
            ))

            logging.debug('Running query:\n    %s' % (diff_query.replace('\n', '\n' + ' ' * 4)))
            diff_query_ref = self.destination_table.get_client().query(diff_query, job_config=diff_query_config)

            # Wait for the diff query to run.
            diff_query_result = diff_query_ref.result()

            logging.info('Diff query quota usage: %s' % format_bytes(diff_query_ref.total_bytes_billed))
        else:
            # We still need an iterator to all the rows in the diff table.
            diff_it_query = self.destination_table.get_client().query(
                'SELECT * FROM %s' % diff_table.get_full_name(standard=True, quoted=True)
            )

            diff_query_result = diff_it_query.result()

            logging.info('Diff iterator query quota usage: %s' % format_bytes(diff_it_query.total_bytes_billed))

        if diff_table.get_table().num_rows == 0:
            logging.info('Tables already synced!')
        else:
            logging.info('Copying %d rows from the diff table (%s) to the destination table (%s).' %
                         (diff_table.get_table().num_rows, diff_table.get_full_name(),
                          self.destination_table.get_full_name()))

            target_client = self.destination_table.get_client()

            copy_job_config = CopyJobConfig()
            copy_job_config.write_disposition = 'WRITE_APPEND'

            copy_job = target_client.copy_table(sources=[diff_table.reference],
                                                destination=self.destination_table.reference,
                                                job_config=copy_job_config)

            copy_job_result = copy_job.result()

            if copy_job_result.errors is not None:
                raise RuntimeError('Copy job failed, reason: \n%s' % copy_job_result.errors)

            # target_table = self.destination_table.reference
            # insert_buffer = []
            # with tqdm(total=diff_table.get_table().num_rows, desc='Inserting diff') as progress_bar:
            #     for row in diff_query_result:
            #         insert_buffer.append(row)
            #
            #         if len(insert_buffer) >= self.insert_buffer_size:
            #             errors = target_client.insert_rows(target_table, insert_buffer,
            #                                                selected_fields=diff_table.schema)
            #             assert errors == []
            #
            #             progress_bar.update(len(insert_buffer))
            #             insert_buffer = []
            #
            #     if len(insert_buffer) > 0:
            #         errors = target_client.insert_rows(target_table, insert_buffer,
            #                                            selected_fields=diff_table.schema)
            #         assert errors == []
            #
            #         progress_bar.update(len(insert_buffer))

            logging.info('Done syncing!')

        logging.info('Cleaning up intermediate tables.')

        partial_slice_table.delete(prompt=False)
        diff_table.delete(prompt=False)

        logging.info('Done!')

    def __build_diff_query__(self, partial_slice_table):

        dest_table = self.destination_table.get_table()

        if dest_table.time_partitioning.type_ != 'DAY':
            raise RuntimeError('Destination table %s needs to be partitioned by day' %
                               self.destination_table.get_full_name())

        partitioning_field = dest_table.time_partitioning.field

        if partitioning_field is None:
            partitioning_field = '_PARTITIONTIME'

        partitions_query_config = QueryJobConfig()
        partitions_query_config.use_legacy_sql = True
        partitions_query = 'SELECT partition_id FROM [%s$__PARTITIONS_SUMMARY__]' % \
                           (self.destination_table.get_full_name())
        logging.info('Finding the latest partition of the destination table (%s).' %
                     self.destination_table.get_full_name())

        logging.debug('Running query:\n    %s' % (partitions_query.replace('\n', '\n' + ' ' * 4)))

        partitions_query = self.destination_table.get_client().query(partitions_query,
                                                                     job_config=partitions_query_config)

        partition_ids = [row['partition_id'] for row in partitions_query.result()]
        max_non_null_partition = max([p_id for p_id in partition_ids if p_id not in ['__NULL__', '__UNPARTITIONED__']])

        last_partition = datetime.strptime(max_non_null_partition, '%Y%m%d')

        utc_now = datetime.utcnow()
        decorator_time_back = utc_now - self.destination_table.get_table().created.replace(tzinfo=None)
        time_back_utc = utc_now - timedelta(seconds=math.ceil(decorator_time_back.total_seconds() * 2))

        if time_back_utc < last_partition:
            last_partition = time_back_utc

        diff_query = 'SELECT {source_fields} \n' \
                     'FROM `{partial_table_full_name}` AS source_partial \n' \
                     'LEFT JOIN (\n' \
                     '    SELECT `{target_fields}` FROM `{target_table_full_name}`\n' \
                     '    WHERE `{partitioning_field}` >= "{latest_partition}" OR `{partitioning_field}` IS NULL\n' \
                     ') AS target_table ON \n{join_predicate} \n' \
                     'WHERE {where_predicate}'

        join_predicate = ' ' * 10 + (' AND \n' + ' ' * 10).join([
            join_comparison[f.field_type].format(
                source_field='source_partial.`%s`' % f.name,
                target_field='target_table.`%s`' % f.name
            )
            for f in self.destination_table.schema if f.name in self.pkey
        ])

        where_predicate = (' AND \n' + ' ' * 6).join([
            'target_table.`{field_name}` IS NULL'.format(field_name=f.name)
            for f in self.destination_table.schema if f.name in self.pkey
        ])

        return diff_query.format(
            source_fields=', '.join(['source_partial.`%s`' % f for f in self.sync_fields]),
            partial_table_full_name=partial_slice_table.get_full_name(standard=True),
            target_fields='`, `'.join(self.sync_fields),
            target_table_full_name=self.destination_table.get_full_name(standard=True),
            partitioning_field=partitioning_field,
            latest_partition=last_partition.strftime('%Y-%m-%d 00:00:00'),
            join_predicate=join_predicate,
            where_predicate=where_predicate
        )
