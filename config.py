import os
_basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    CONNECTION_TIMEOUT = 10.0

    MIN_ACCOUNT_ID = 1000000000
    MAX_ACCOUNT_ID = 1100000000
    MAX_BATCH_SIZE = 100

    WOT_APPLICATION_ID = 'demo'

