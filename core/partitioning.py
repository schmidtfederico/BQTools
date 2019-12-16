from abc import abstractmethod

import logging
from google.cloud.bigquery import TimePartitioning, QueryJobConfig, CopyJobConfig

from core.bq import BQTable, format_bytes
from tqdm import tqdm


class Partitioning:
    def __init__(self, source_table, destination_table, source_fields=None, excluded_fields=None):
        """
        :param source_table: A representation of the source table.
        :type source_table: :class:`~core.bq.BQTable`

        :param destination_table: A representation of the destination table.
        :type destination_table: :class:`~core.bq.BQTable`

        :param source_fields: A list of fields/columns to copy from the source table to the destination table.
        :type source_fields: list

        :param excluded_fields: A list of fields/columns to exclude from the destination table.
        :type excluded_fields: list
        """
        self.source_table = source_table
        self.destination_table = destination_table
        table_fields = set(self.source_table.get_fields_map().keys())

        if source_fields is None:
            source_fields = table_fields

        source_fields = set(source_fields)

        if excluded_fields is not None:
            excluded_fields = set(excluded_fields)

            if not table_fields.issuperset(excluded_fields):
                raise RuntimeError('excluded_fields should be a subset of the source table columns (not found: %s)' %
                                   list(excluded_fields - table_fields))

            source_fields -= excluded_fields

        if not source_fields.issubset(table_fields):
            raise RuntimeError('source_fields should be a subset of the source table columns (not found: %s)' %
                               list(source_fields - table_fields))

        self.source_fields = [f for f in table_fields if f in source_fields]  # Make sure column order is preserved.

        if len(self.source_fields) < len(table_fields):
            logging.info('Selecting only a subset of columns from the source table (%s)' %
                         ', '.join(self.source_fields))

        if len(self.source_fields) < 1:
            raise RuntimeError('All source table fields were excluded from target table')

    def run(self):
        if self.destination_table.exists():
            logging.warning('Destination table (%s) already exists. We need to delete it to proceed.' %
                            self.destination_table.get_full_name())
            delete_result = self.destination_table.delete(prompt=True)

            if not delete_result:
                logging.error('Destination table already exists, aborting.')
                exit(1)

        # Create destination table, but let each subclass define how this table should be partitioned.
        self.destination_table.create(schema=[f for f in self.source_table.schema if f.name in self.source_fields],
                                      partitioning=self.__get_partitioning__())
        logging.info('Created destination table %s' % self.destination_table.get_full_name())

        self.__do_preprocessing__()

        load_query = self.__get_load_query__()

        logging.debug('Running query:\n    %s' % (load_query.replace('\n', '\n' + ' ' * 4)))

        load_query_ref = self.destination_table.get_client().query(load_query)
        load_query_ref.result()

        logging.info('Load query quota usage: %s' % format_bytes(load_query_ref.total_bytes_billed))

        self.__do_postprocessing__()

    @abstractmethod
    def __do_preprocessing__(self):
        pass

    @abstractmethod
    def __do_postprocessing__(self):
        pass

    @abstractmethod
    def __get_partitioning__(self):
        pass

    @abstractmethod
    def __get_load_query__(self):
        pass


class FieldPartitioning(Partitioning):

    def __init__(self, source_table, destination_table, source_fields=None, partitioning_field=None,
                 excluded_fields=None):
        Partitioning.__init__(self, source_table, destination_table, source_fields, excluded_fields)
        self.partitioning_field = partitioning_field

        source_table_fields = source_table.get_fields_map()
        if self.partitioning_field not in source_table_fields:
            raise RuntimeError('Partitioning field should be an existing field in the source table.')

        if source_table_fields[self.partitioning_field] not in ['TIMESTAMP', 'DATE']:
            raise RuntimeError('The partitioning_field should be of type TIMESTAMP or DATE in the source table')

    def __get_partitioning__(self):
        return TimePartitioning(field=self.partitioning_field)

    def __get_load_query__(self):
        return 'INSERT INTO `{target_table}` (`{source_fields}`)\nSELECT `{source_fields}` FROM `{source_table}`'.format(
            target_table=self.destination_table.get_full_name(standard=True),
            source_fields='`, `'.join(self.source_fields),
            source_table=self.source_table.get_full_name(standard=True)
        )

    def __do_preprocessing__(self):
        pass

    def __do_postprocessing__(self):
        pass


class InsertionTimePartitioning(Partitioning):

    def __init__(self, source_table, destination_table, source_fields=None, insertion_timestamp_field=None,
                 excluded_fields=None):
        Partitioning.__init__(self, source_table, destination_table, source_fields, excluded_fields)
        self.insertion_timestamp_field = insertion_timestamp_field

        source_table_fields = source_table.get_fields_map()
        if self.insertion_timestamp_field not in source_table_fields:
            raise RuntimeError(
                'When partitioning by insertion time (_PARTITIONTIME), the field provided as the insertion '
                'timestamp should be an existing field in the source table (insertion_ts_field: %s)' %
                self.insertion_timestamp_field)

        if source_table_fields[self.insertion_timestamp_field] != 'TIMESTAMP':
            raise RuntimeError('The insertion_timestamp_field should be of type TIMESTAMP in the source table')

        # We will use a temporal target table, partitioned by a field that is as close as possible to the real insertion
        # time of each record (defined by the user).
        self.tmp_target_table = BQTable(destination_table.project, destination_table.dataset,
                                        name=destination_table.name + '_tmp')

    def __do_preprocessing__(self):
        # If the temportal table already exists, possibly caused by a failure in previous executions, delete it.
        if self.tmp_target_table.exists():
            self.tmp_target_table.delete(prompt=False)

        self.tmp_target_table.create(schema=self.destination_table.schema,
                                     partitioning=TimePartitioning(field=self.insertion_timestamp_field))
        logging.info('Created temporal target table "%s"' % self.tmp_target_table.get_full_name())

    def __get_load_query__(self):
        return 'INSERT INTO `{target_table}` (`{source_fields}`)\nSELECT `{source_fields}` FROM `{source_table}`'.format(
            target_table=self.tmp_target_table.get_full_name(standard=True),
            source_fields='`, `'.join(self.source_fields),
            source_table=self.source_table.get_full_name(standard=True)
        )

    def __get_partitioning__(self):
        return TimePartitioning()

    def __do_postprocessing__(self):
        partitions_query_config = QueryJobConfig()
        partitions_query_config.use_legacy_sql = True

        partitions_query = 'SELECT partition_id FROM [%s$__PARTITIONS_SUMMARY__]' % \
                           self.tmp_target_table.get_full_name()

        logging.debug('Running query:\n    %s' % (partitions_query.replace('\n', '\n' + ' ' * 4)))

        partitions_query = self.tmp_target_table.get_client().query(partitions_query,
                                                                    job_config=partitions_query_config)

        partition_ids = [row['partition_id'] for row in partitions_query.result()]
        max_non_null_partition = max([p_id for p_id in partition_ids if p_id not in ['__NULL__', '__UNPARTITIONED__']])

        target_client = self.destination_table.get_client()

        copy_job_config = CopyJobConfig()
        copy_job_config.write_disposition = 'WRITE_APPEND'

        for partition_id in tqdm(partition_ids, desc='Copying partitions'):
            shard_id = partition_id

            if partition_id in ['__UNPARTITIONED__', '__NULL__']:
                # Insert all other partitions in the latest partition, losing the real insertion time in the process.
                shard_id = max_non_null_partition

            source_partition = BQTable(project=self.tmp_target_table.project, dataset=self.tmp_target_table.dataset,
                                       name=self.tmp_target_table.name, partition=partition_id)

            target_partition = BQTable(project=self.destination_table.project, dataset=self.destination_table.dataset,
                                       name=self.destination_table.name, partition=shard_id)

            # Do the actual copying.
            copy_job = target_client.copy_table(sources=[source_partition.reference],
                                                destination=target_partition.reference,
                                                job_config=copy_job_config)

        logging.info('Done partitioning!')

        self.tmp_target_table.delete(prompt=True)
