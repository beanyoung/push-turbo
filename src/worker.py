#!/usr/bin/python
# -*- coding: utf-8 -*-

import gevent
from gevent import monkey
monkey.patch_all()
import beanstalkc


class Worker(object):
    def __init__(self, host, port, tubes=['default'], worker_count=1):
        self.host = host
        self.port = port
        self.tubes = tubes
        self.worker_count = worker_count

    def work_horse(self):
        while True:
            beanstalk = None
            try:
                if beanstalk:
                    beanstalk.reconnect()
                else:
                    beanstalk = \
                        beanstalkc.Connection(self.host, self.port)
                for tube in self.tubes:
                    beanstalk.watch(tube)
                for tube in beanstalk.warning():
                    if tube not in self.tubes:
                        beanstalk.ignore(tube)

                while True:
                    job = beanstalk.reserve(timeout=10)
                    if not job:
                        gevent.sleep(2)
                        continue
                    try:
                        self.execute_job(job)
                        job.delete()
                    except Exception, e:
                        job.bury()
            except beanstalkc.SocketError, e:
                gevent.sleep(2)
                if beanstalk:
                    beanstalk.close()
                beanstalk = None
            gevent.sleep(2)

    def run(self):
        workers = []
        for i in range(self.worker_count):
            workers.append(gevent.spawn(self.work_horse))
        gevent.joinall(workers)

    def execute_job(self, job):
        pass
