#!/usr/bin/python
# -*- coding: utf-8 -*-

import json

import gevent

import config
from worker import Worker
from turbo import Pipe


pipes = dict()


class PushWorker(Worker):
    def execute_job(self, job):
        job_body = json.loads(job.body)
        if job_body['app_name'] not in pipes:
            return
        pipes[job_body['app_name']].send(job_body)


if __name__ == '__main__':
    for app_name, key_file in config.APPS.items():
        print key_file
        pipes[app_name] = Pipe(
            key_file[0], key_file[1], False, config.PIPE_CONNECTION_COUNT)
        pipes[app_name].start()

    worker = PushWorker(
        config.BEANSTALKD_HOST, config.BEANSTALKD_PORT,
        'push', config.WORKER_COUNT)
    worker.run()
