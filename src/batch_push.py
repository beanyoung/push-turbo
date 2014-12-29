#!/usr/bin/python
# -*- coding: utf-8 -*-

import beanstalkc

import config
from worker import Worker


class BatchPushWorker(Worker):
    def execute_job(self, job):
        jobs = json.loads(job.body)
        beanstalk = beanstalkc.Connection(
            host=config.BEANSTALKD_HOST, port=config.BEANSTALKD_PORT)
        beanstalk.use('push')
        for job in jobs:
            priority = priorities.get(job.get('priority', 'low'))
            beanstalk.put(json.dumps(job), priority=priority)
        beanstalk.close()


if __name__ == '__main__':
    worker = PushWorker(
        config.BEANSTALKD_HOST,
        config.BEANSTALKD_PORT,
        ['batch_push'],
        config.WORKER_COUNT)
    worker.run()
