#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import logging

import beanstalkc
import gevent
from gevent import monkey
monkey.patch_all()


PRIORITIES = dict(low=4294967295, normal=2147483647, high=0)


def batch_push(host, port, watching_tube, using_tube):
    while True:
        # init beanstalk
        try:
            beanstalk = beanstalkc.Connection(host, port)
            logging.info('Connect to %s:%s success' % (host, port))
            beanstalk.watch(watching_tube)
            for tube in beanstalk.watching():
                if tube != watching_tube:
                    beanstalk.ignore(tube)
            beanstalk.use(using_tube)
        except beanstalkc.SocketError:
            logging.warning('Connect to %s:%s failed' % (host, port))
            gevent.sleep(2)
            continue

        try:
            while True:
                job = beanstalk.reserve(timeout=10)
                if not job:
                    logging.info('No job found. Sleep 2 seconds')
                    gevent.sleep(2)
                    continue
                logging.info('Reserved job: %s' % job.jid)
                logging.debug('Reserved job: %s' % job.body)
                try:
                    push_tasks = json.loads(job.body)
                except ValueError:
                    logging.error('Failed to loads job body: %s'% job.body)
                    job.bury()

                for push_task in push_tasks:
                    priority = PRIORITIES.get(push_task.get('priority', 'low'))
                    delay = push_task.get('delay', 0)
                    beanstalk.put(
                        json.dumps(push_task), priority=priority, delay=delay)
                job.delete()
                logging.info('Delete job: %s' % job.jid)
                logging.debug('Delete job: %s' % job.body)
        except beanstalkc.SocketError:
            logging.warning('Server %s:%s is down' % (host, port))
            gevent.sleep(2)
            continue



if __name__ == '__main__':
    import config
    logger = logging.getLogger()
    logger.setLevel(config.LOGGING_LEVEL)
    batch_push(
        config.BEANSTALKD_HOST,
        config.BEANSTALKD_PORT,
        config.BATCH_PUSH_TUBE,
        config.PUSH_TUBE)
