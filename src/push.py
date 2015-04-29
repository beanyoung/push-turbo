#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import logging
from multiprocessing import Process, Queue
from Queue import Empty
import time
from threading import Thread

import beanstalkc


PRIORITIES = dict(low=4294967295, normal=2147483647, high=0)


def ios_push_out(queue):
    # multithread
    while True:
        try:
            i = queue.get(timeout=10)
            logging.info('Get %s' % i)
        except Empty as e:
            logging.info('Nothing got')


def ios_push_in(queue, host, port, watching_tube):
    while True:
        # init beanstalk
        try:
            beanstalk = beanstalkc.Connection(host, port)
            logging.info('Connect to %s:%s success' % (host, port))
            beanstalk.watch(watching_tube)
            for tube in beanstalk.watching():
                if tube != watching_tube:
                    beanstalk.ignore(tube)
        except beanstalkc.SocketError:
            logging.warning('Connect to %s:%s failed' % (host, port))
            time.sleep(2)
            continue

        try:
            while True:
                job = beanstalk.reserve(timeout=10)
                if not job:
                    logging.info('No job found')
                    continue
                logging.info('Reserved job: %s' % job.jid)
                logging.debug('Reserved job: %s' % job.body)
                try:
                    push_task = json.loads(job.body)
                except ValueError:
                    logging.error('Failed to loads job body: %s'% job.body)
                    job.bury()

                queue.put(job.body)

                job.delete()
                logging.info('Delete job: %s' % job.jid)
                logging.debug('Delete job: %s' % job.body)
        except beanstalkc.SocketError:
            logging.warning('Server %s:%s is down' % (host, port))
            time.sleep(2)
            continue


if __name__ == '__main__':
    import config
    logger = logging.getLogger()
    logger.setLevel(config.LOGGING_LEVEL)
    queue = Queue()
    for i in range(10):
        p = Process(target=ios_push_out, args=(queue,))
        p.start()

    for i in range(10):
        args = (queue, config.BEANSTALKD_HOST,
            config.BEANSTALKD_PORT, config.PUSH_TUBE)
        t = Thread(target=ios_push_in, args=args)
        t.start()
