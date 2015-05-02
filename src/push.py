#!/usr/bin/python
# -*- coding: utf-8 -*-

from gevent import monkey
monkey.patch_all()

import json
import logging
import time
from threading import Thread
import select
import ssl

import beanstalkc

import apns


PRIORITIES = dict(low=4294967295, normal=2147483647, high=0)


class Pipe(object):
    def __init__(
            self, beanstalkd_host, beanstalkd_port, watching_tube,
            gateway_host, gateway_port, key_file, cert_file):
        self.beanstalkd_host = beanstalkd_host
        self.beanstalkd_port = beanstalkd_port
        self.watching_tube = watching_tube
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port
        self.key_file = key_file
        self.cert_file = cert_file

        self.push_id = 0
        self.pushed_buff = []
        self.beanstalk = None
        self.gateway_connection = None
        self.gateway_invalid = False

    def init_beanstalk(self):
        # init beanstalk
        while True:
            try:
                if self.beanstalk:
                    self.beanstalk.close()

                self.beanstalk = beanstalkc.Connection(
                    self.beanstalkd_host, self.beanstalkd_port)
                logging.info('Connect to %s:%s success' % (host, port))
                self.beanstalk.watch(watching_tube)
                for tube in self.beanstalk.watching():
                    if tube != watching_tube:
                        self.beanstalk.ignore(tube)
            except beanstalkc.SocketError:
                logging.warning('Connect to %s:%s failed' % (host, port))
                time.sleep(2)
                continue

    def init_gateway(self):
        while True:
            try:
                if not self.gateway_connection:
                    self.gateway_connection = apns.GatewayConnection(
                        host=self.gateway_host,
                        port=self.gateway_port,
                        cert_file=self.cert_file,
                        key_file=self.key_file,
                    )
                else:
                    gateway_connection.reconnect()
            except ssl.SSLError as e:
                if e.errno == ssl.SSL_ERROR_SSL:
                    self.gateway_invalid = True
                    time.sleep(3600)
                    logging.warning('Invalid key: %s' % self.key_file)
            except (socket.error, IOError) as e:
                pass
            logging.warning('Connect to gateway error')
            time.sleep(2)
            continue


    def process_gateway_input(self):
        pass

    def push_job(self):
        job = self.beanstalk.reserve(timeout=2)
        if not job:
            logging.info('No job found')
            return
        logging.info('Reserved job: %s' % job.jid)
        logging.debug('Reserved job: %s' % job.body)
        try:
            job_body = json.loads(job.body)
        except ValueError:
            logging.error('Failed to loads job body: %s'% job.body)
            job.bury()

        # push job
        self.push_id += 1
        self.gateway_connection.send_notification(
            job_body['device_token'],
            Payload(**job_body['payload']),
            self.push_id)
        if self.pushed_buffer.full():
            self.pushed_buffer.get()
        self.pushed_buffer.put((push_id, job_body))

        job.delete()
        logging.info('Delete job: %s' % job.jid)
        logging.debug('Delete job: %s' % job.body)


    def reserve_and_push(self):
        while True:
            rlist, wlist, _ = select.select(
                [gateway_connection.connection()],
                [gateway_connection.connection()],
                [],
                10)
            if rlist:
                self.process_gateway_input()
            elif wlist:
                self.push_job()

    def run(self):
        self.init_beanstalk()
        self.init_gateway()

        while True:
            try:
                self.reserve_and_push()
            except beanstalkc.SocketError:
                self.init_beanstalk()
            except (ssl.SSLError, socket.error, IOError) as e:
                self.init_gateway()


if __name__ == '__main__':
    import config
    logger = logging.getLogger()
    logger.setLevel(config.LOGGING_LEVEL)
    key_file = ''
    cert_file = ''
    pipe = Pipe(
        config.BEANSTALKD_HOST, config.BEANSTALKD_PORT, config.PUSH_TUBE,
        config.APNS_HOST, config.APNS_PORT, key_file, cert_file)
    pipe.run()
