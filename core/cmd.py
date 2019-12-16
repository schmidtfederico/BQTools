import os
import re

import logging

table_name_regex = re.compile(r'(.*)[:.](.*)\.(.*)')


def setup_logging(verbose):
    log_level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(level=log_level,
                        format='%(asctime)s - %(name)s - %(module)s:%(lineno)s - %(levelname)s - %(message)s')
    logging.getLogger('urllib3').setLevel(logging.ERROR)
    logging.getLogger('google').setLevel(logging.INFO)


def parse_table_name(args, name):
    (project, dataset, table_name) = table_name_regex.match(args).groups()

    if project is None or dataset is None or name is None:
        raise RuntimeError('Incomplete %s table name, a full name should be provided (project:dataset.table)' % name)

    return project, dataset, table_name


def load_api_key():
    key_path = os.path.normpath(os.path.join('data', 'auth', 'bq_key.json'))

    if not os.path.exists(key_path):
        raise RuntimeError('Missing Google Service Account Credentials at data/auth/bq_key.json')

    # Set auth key path before loading BQ lib.
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
