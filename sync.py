# coding=utf-8
from __future__ import print_function
import argparse

from core.bq import BQTable
from core.cmd import parse_table_name, setup_logging
from core.syncing import Syncing

parser = argparse.ArgumentParser(description='BigQuery table partitioning')

parser.add_argument(metavar='source_table', dest='source', type=str,
                    help='the source table full name (i.e. project:Dataset.Source)')
parser.add_argument(metavar='target_table', dest='target', type=str,
                    help='the target/destination table full name (i.e. project:Dataset.SourcePartitioned)')
parser.add_argument('--pkey', type=str, default=None, help='the comma-separated list of fields that make the primary '
                                                           'key (default: all fields)')

parser.add_argument('--insert_buffer_size', type=int, default=1000, help='the batch size for streaming rows into the '
                                                                         'destination table (default: 1000)')
parser.add_argument('-v', '--verbose', dest='verbose', const=True, action='store_const', default=False)

if __name__ == '__main__':
    args = parser.parse_args()

    setup_logging(args.verbose)

    (source_project, source_dataset, source_name) = parse_table_name(args.source, name='source')

    (target_project, target_dataset, target_name) = parse_table_name(args.target, name='destination')

    source_table = BQTable(source_project, source_dataset, source_name)
    target_table = BQTable(target_project, target_dataset, target_name)

    buffer_size = args.insert_buffer_size
    primary_key = args.pkey

    if primary_key is not None:
        primary_key = [f.strip() for f in primary_key.split(',')]

    sync = Syncing(source_table, target_table, pkey=primary_key, insert_buffer_size=buffer_size)

    sync.run()
