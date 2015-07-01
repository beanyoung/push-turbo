#!/usr/bin/python
# -*- coding: utf-8 -*-

DEBUG = TRUE

APPS = {
    'demo_app_name': ('/path/to/cert', '/path/to/key', 10),
}

BATCH_WORKER_COUNT = 10
BEANSTALKD_HOST = '127.0.0.1'
BEANSTALKD_PORT = 11300

PRIORITIES = dict(low=4294967295, normal=2147483647, high=0)

PUSH_TUBE = 'ios_push.%s'
BATCH_PUSH_TUBE = 'ios_batch_push'

LOGGING_LEVEL = 10
LOGGING_FORMAT = \
    '%(asctime)s - %(levelname)s - %(threadName)s - %(funcName)s - %(message)s'
LOGGING_HANDLERS = []

APNS_HOST = '127.0.0.1'
APNS_PORT = 2190
