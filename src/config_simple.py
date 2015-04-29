#!/usr/bin/python
# -*- coding: utf-8 -*-

DEBUG = TRUE

APPS = {
    'demo_app_name': ('/path/to/cert', '/path/to/key'),
}

PIPE_CONNECTION_COUNT = 15
WORKER_COUNT = 15
BEANSTALKD_HOST = '127.0.0.1'
BEANSTALKD_PORT = 11300

PRIORITIES = dict(low=4294967295, normal=2147483647, high=0)

PUSH_TUBE = 'push'
BATCH_PUSH_TUBE = 'batch_push'

LOGGING_LEVEL = 10
