#!/usr/bin/python
# -*- coding: utf-8 -*-

import json

import beanstalkc

import config
from worker import Worker


class BatchPushWorker(Worker):
    def execute_job(self, job):
        jobs = json.loads(job.body)
        beanstalk = beanstalkc.Connection(
            host=config.BEANSTALKD_HOST, port=config.BEANSTALKD_PORT)
        beanstalk.use(config.PUSH_TUBE)
        for job in jobs:
            priority = config.PRIORITIES.get(job.get('priority', 'low'))
            beanstalk.put(json.dumps(job), priority=priority)
        beanstalk.close()


if __name__ == '__main__':
    worker = BatchPushWorker(
        config.BEANSTALKD_HOST,
        config.BEANSTALKD_PORT,
        [config.BATCH_PUSH_TUBE],
        config.WORKER_COUNT)
    worker.run()
