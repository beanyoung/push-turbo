#!/usr/bin/python
# -*- coding: utf-8 -*-

from gevent import monkey
monkey.patch_all()

import json
import logging
import Queue
import select
import socket
import ssl
import time
from threading import Thread

import beanstalkc

import apns


PRIORITIES = dict(low=4294967295, normal=2147483647, high=0)


class Pipe(object):
    def __init__(
            self, beanstalkd_host, beanstalkd_port, tube,
            gateway_host, gateway_port, key_file, cert_file):
        self.beanstalkd_host = beanstalkd_host
        self.beanstalkd_port = beanstalkd_port
        self.tube = tube
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port
        self.key_file = key_file
        self.cert_file = cert_file

        self.push_id = 0
        self.pushed_buffer = Queue.Queue(maxsize=1000)
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
                logging.info(
                    '%s: Connect to %s:%s success' % (
                        self.tube, self.beanstalkd_host, self.beanstalkd_port))
                self.beanstalk.watch(self.tube)
                for tube in self.beanstalk.watching():
                    if tube != self.tube:
                        self.beanstalk.ignore(tube)
                self.beanstalk.use(tube)
                return
            except beanstalkc.SocketError:
                logging.warning(
                    '%s: Connect to %s:%s failed' % (
                        self.tube, self.beanstalkd_host, self.beanstalkd_port))
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
                    self.gateway_connection.reconnect()
                return
            except ssl.SSLError as e:
                if e.errno == ssl.SSL_ERROR_SSL:
                    self.gateway_invalid = True
                    time.sleep(3600)
                    logging.warning('%s: Invalid key' % self.tube)
            except (socket.error, IOError) as e:
                logging.warning(
                    '%s: Gateway connect error %s' % (self.tube, e))
            time.sleep(2)
            continue


    def process_gateway_input(self):
        buff = self.gateway_connection.read(apns.ERROR_RESPONSE_LENGTH)
        if len(buff) == apns.ERROR_RESPONSE_LENGTH:
            command, status, error_identifier = \
                apns.unpack(apns.ERROR_RESPONSE_FORMAT, buff)

            if 8 == command:
                found = False
                while not self.pushed_buffer.empty():
                    identifier, job = self.pushed_buffer.get()
                    if found:
                        logging.debug(
                            '%s: Reput failed job %s' % (self.tube, identifier))
                        self.beanstalk.put(json.dumps(job))
                    elif identifier == error_identifier:
                        logging.debug(
                            '%s: Found error identifier %s' % (self.tube, identifier))
                        found = True
        elif len(buff) == 0:
            logging.info('%s: Close by server' % self.tube)
        else:
            logging.error(
                '%s: Unexcepted read buf size %s' % (self.tube, len(buf)))

    def push_job(self):
        job = self.beanstalk.reserve(timeout=2)
        if not job:
            logging.debug('%s: No job found' % self.tube)
            return
        logging.debug('%s: Reserved job: %s' % (self.tube, job.body))
        try:
            job_body = json.loads(job.body)
        except ValueError:
            logging.error('%s: Failed to loads job body: %s'% (self.tube, job.body))
            job.bury()

        # push job
        self.push_id += 1
        self.gateway_connection.send_notification(
            job_body['device_token'],
            apns.Payload(**job_body['payload']),
            self.push_id)
        if self.pushed_buffer.full():
            self.pushed_buffer.get()
        self.pushed_buffer.put((self.push_id, job_body))

        job.delete()
        logging.debug('%s: Delete job: %s %s' % (self.tube, job.jid, job.body))


    def reserve_and_push(self):
        while True:
            rlist, wlist, _ = select.select(
                [self.gateway_connection.connection()],
                [self.gateway_connection.connection()],
                [],
                10)
            if rlist:
                logging.debug('%s: Start Reading from gateway' % self.tube)
                self.process_gateway_input()
                self.gateway_connection.reconnect()
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
    logging.basicConfig(format='%(asctime)s - %(levelname)- %(threadName)s.%(funcName)s - %(message)s', level=config.LOGGING_LEVEL)
    key_file = '../pems/martin_new_key.pem'
    cert_file = '../pems/martin_new_cert.pem'
    for i in range(10):
        pipe = Pipe(
            config.BEANSTALKD_HOST, config.BEANSTALKD_PORT, config.PUSH_TUBE,
            config.APNS_HOST, config.APNS_PORT, key_file, cert_file)
        t = Thread(target=pipe.run, name=str(i))
        t.start()
