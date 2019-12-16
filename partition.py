# coding=utf-8
from __future__ import print_function
import argparse

from core.bq import BQTable
from core.cmd import setup_logging, parse_table_name
from core.partitioning import InsertionTimePartitioning, FieldPartitioning

parser = argparse.ArgumentParser(description='BigQuery table partitioning')

parser.add_argument(metavar='source_table', dest='source', type=str,
                    help='the source table full name (i.e. project:Dataset.Source)')
parser.add_argument(metavar='target_table', dest='target', type=str,
                    help='the target/destination table full name (i.e. project:Dataset.SourcePartitioned)')
parser.add_argument('-t', dest='partitioning_type', choices=['by_insertion_ts', 'by_field'], default='by_insertion_ts',
                    help='the type of partitioned table that we should create from source '
                         '(default: by insertion timestamp)', type=str)
parser.add_argument('--partitioning_field', type=str, default='ts', help='the field to use as the new partition key, '
                                                                         'only needed when partitioning by_field '
                                                                         '(default: ts)')
parser.add_argument('--insertion_timestamp_field', type=str, default='ts',
                    help='the field to use as the insertion timestamp of each record of the source table, '
                         'only needed when partitioning by_insertion_ts (default: ts)')
parser.add_argument('--fields', metavar='source_fields', dest='source_fields', type=str, default=None, required=False,
                    help='the comma-separated list of columns from the source table to include in the target table '
                         '(default: all)')
parser.add_argument('--excluded_fields', metavar='excluded_fields', dest='excluded_fields', type=str, default=None,
                    required=False, help='the comma-separated list of columns from the source table to exclude from '
                                         'the target table (default: None)')
parser.add_argument('-v', '--verbose', dest='verbose', const=True, action='store_const', default=False)

if __name__ == '__main__':
    args = parser.parse_args()

    setup_logging(args.verbose)

    (source_project, source_dataset, source_name) = parse_table_name(args.source, name='source')

    (target_project, target_dataset, target_name) = parse_table_name(args.target, name='destination')

    source_table = BQTable(source_project, source_dataset, source_name)
    target_table = BQTable(target_project, target_dataset, target_name)

    source_fields = args.source_fields
    if source_fields is not None:
        source_fields = [f.strip() for f in source_fields.split(',')]

    excluded_fields = args.excluded_fields
    if excluded_fields is not None:
        excluded_fields = [f.strip() for f in excluded_fields.split(',')]

    if args.partitioning_type == 'by_insertion_ts':
        partitioning_strategy = InsertionTimePartitioning(source_table, target_table,
                                                          source_fields=source_fields,
                                                          excluded_fields=excluded_fields,
                                                          insertion_timestamp_field=args.insertion_timestamp_field)
    else:
        partitioning_strategy = FieldPartitioning(source_table, target_table,
                                                  source_fields=source_fields,
                                                  excluded_fields=excluded_fields,
                                                  partitioning_field=args.partitioning_field)

    partitioning_strategy.run()
