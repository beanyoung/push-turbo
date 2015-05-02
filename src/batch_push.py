#!/usr/bin/python
# -*- coding: utf-8 -*-

from gevent import monkey
monkey.patch_all()

import json
import logging
import time

import beanstalkc

import config


def batch_push(host, port, watching_tube, using_tube):
    logging.debug('Starting')
    while True:
        # init beanstalk
        try:
            beanstalk = beanstalkc.Connection(host, port)
            logging.debug('Connect to %s:%s success' % (host, port))
            beanstalk.watch(watching_tube)
            for tube in beanstalk.watching():
                if tube != watching_tube:
                    beanstalk.ignore(tube)
        except beanstalkc.SocketError:
            logging.debug('Connect to %s:%s failed' % (host, port))
            time.sleep(2)
            continue

        try:
            while True:
                job = beanstalk.reserve(timeout=10)
                if not job:
                    logging.debug('No job found. Sleep 2 seconds')
                    time.sleep(2)
                    continue
                logging.debug('Reserved job: %s %s' % (job.jid, job.body))
                try:
                    push_jobs = json.loads(job.body)
                except ValueError:
                    logging.debug(
                        'Failed to load job body: %s %s' % (job.jid, job.body))
                    job.bury()

                current_using = ''
                for push_job in push_jobs:
                    priority = config.PRIORITIES.get(
                        push_job.get('priority', 'low'))
                    delay = push_job.get('delay', 0)
                    next_using = config.PUSH_TUBE % push_job['app_name']
                    if next_using != current_using:
                        beanstalk.use(next_using)
                    beanstalk.put(
                        json.dumps(push_job), priority=priority, delay=delay)
                job.delete()
                logging.debug('Delete job: %s %s' % (job.jid, job.body))
        except beanstalkc.SocketError:
            logging.debug('Server %s:%s is down' % (host, port))
            time.sleep(2)
            continue


if __name__ == '__main__':
    logging.basicConfig(
        format=config.LOGGING_FORMAT, level=config.LOGGING_LEVEL)
    batch_push(
        config.BEANSTALKD_HOST,
        config.BEANSTALKD_PORT,
        config.BATCH_PUSH_TUBE,
        config.PUSH_TUBE)
