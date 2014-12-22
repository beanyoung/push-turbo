#!/usr/bin/python
# -*- coding: utf-8 -*-

from binascii import a2b_hex, b2a_hex
import copy
import json
from struct import pack, unpack

import gevent
from gevent.queue import Queue
from gevent import ssl, socket, select


ERROR_RESPONSE_LENGTH = 6
ERROR_RESPONSE_FORMAT = (
    '!'  # network big-endian
    'B'  # command
    'B'  # status
    'I'  # identifier
)

ENHANCED_NOTIFICATION_FORMAT = (
    '!'  # network big-endian
    'B'  # command
    'I'  # identifier
    'I'  # expiry
    'H'  # token length
    '32s'  # token
    'H'  # payload length
    '%ds'  # payload
)

ENHANCED_NOTIFICATION_COMMAND = 1
MAX_PAYLOAD_LENGTH = 2048
TOKEN_LENGTH = 32
ERROR_RESPONSE_LENGTH = 6


class APNsConnection(object):
    """
    A generic connection class for communicating with the APNs
    """
    def __init__(self, cert_file=None, key_file=None, timeout=None):
        super(APNsConnection, self).__init__()
        self.cert_file = cert_file
        self.key_file = key_file
        self.timeout = timeout
        self._socket = None
        self._ssl = None
        self.connection_alive = False

    def __del__(self):
        self.disconnect()

    def connect(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self.server, self.port))
        self._ssl = ssl.wrap_socket(
            self._socket,
            self.key_file,
            self.cert_file)
        self.connection_alive = True

    def disconnect(self):
        if self.connection_alive:
            if self._socket:
                self._socket.close()
            if self._ssl:
                self._ssl.close()
            self.connection_alive = False

    def reconnect(self):
        self.disconnect()
        self.connect()

    def connection(self):
        if not self._ssl or not self.connection_alive:
            self.connect()
        return self._ssl

    def read(self, n=None):
        return self.connection().read(n)

    def write(self, string):
        return self.connection().write(string)


class PayloadAlert(object):
    def __init__(
            self, body=None, action_loc_key=None, loc_key=None,
            loc_args=None, launch_image=None):
        super(PayloadAlert, self).__init__()
        self.body = body
        self.action_loc_key = action_loc_key
        self.loc_key = loc_key
        self.loc_args = loc_args
        self.launch_image = launch_image

    def dict(self):
        d = {}
        if self.body:
            d['body'] = self.body
        if self.action_loc_key:
            d['action-loc-key'] = self.action_loc_key
        if self.loc_key:
            d['loc-key'] = self.loc_key
        if self.loc_args:
            d['loc-args'] = self.loc_args
        if self.launch_image:
            d['launch-image'] = self.launch_image
        return d


class Payload(object):
    """A class representing an APNs message payload"""
    def __init__(
            self, alert=None, badge=None, sound=None, category=None,
            custom={}, content_available=False):
        super(Payload, self).__init__()
        self.alert = alert
        self.badge = badge
        self.sound = sound
        self.category = category
        self.custom = custom
        self.content_available = content_available
        self._check_size()

    def dict(self):
        """Returns the payload as a regular Python dictionary"""
        d = {}
        if self.alert:
            if isinstance(self.alert, PayloadAlert):
                d['alert'] = self.alert.dict()
            else:
                d['alert'] = self.alert
        if self.sound:
            d['sound'] = self.sound
        if self.badge is not None:
            d['badge'] = int(self.badge)
        if self.category:
            d['category'] = self.category

        if self.content_available:
            d.update({'content-available': 1})

        d = {'aps': d}
        d.update(self.custom)
        return d

    def json(self):
        return json.dumps(
            self.dict(),
            separators=(',', ':'),
            ensure_ascii=False).encode('utf-8')

    def _check_size(self):
        payload_length = len(self.json())
        if payload_length > MAX_PAYLOAD_LENGTH:
            raise PayloadTooLargeError(payload_length)

    def __repr__(self):
        attrs = ("alert", "badge", "sound", "category", "custom")
        args = ", ".join(["%s=%r" % (n, getattr(self, n)) for n in attrs])
        return "%s(%s)" % (self.__class__.__name__, args)


class PayloadTooLargeError(StandardError):
    def __init__(self, payload_size):
        super(PayloadTooLargeError, self).__init__()
        self.payload_size = payload_size


class GatewayConnection(APNsConnection):
    def __init__(self, use_sandbox=False, **kwargs):
        super(GatewayConnection, self).__init__(**kwargs)
        self.server = (
            'gateway.push.apple.com',
            'gateway.sandbox.push.apple.com')[use_sandbox]
        self.port = 2195

    def get_notification(self, token_hex, payload, identifier, expiry):
        token = a2b_hex(token_hex)
        payload = payload.json()
        fmt = ENHANCED_NOTIFICATION_FORMAT % len(payload)
        notification = pack(
            fmt, ENHANCED_NOTIFICATION_COMMAND, identifier,
            expiry, TOKEN_LENGTH, token, len(payload), payload)
        return notification

    def send_notification(self, token_hex, payload, identifier=0, expiry=0):
        self.write(
            self.get_notification(token_hex, payload, identifier, expiry))


class Pipe(object):
    def __init__(self, cert_file=None, key_file=None, use_sandbox=False):
        super(Pipe, self).__init__()
        self.use_sandbox = use_sandbox
        self.cert_file = cert_file
        self.key_file = key_file
        # init queue
        self.push_queue = Queue(maxsize=1000)
        self.threads = []
        self.invalid = False

    def push_worker(self):
        gateway_connection = GatewayConnection(
            use_sandbox=self.use_sandbox,
            cert_file=self.cert_file,
            key_file=self.key_file,
        )
        gateway_connection.connect()
        pushed_buffer = Queue(maxsize=1000)

        push_id = 0
        while True:
            if self.invalid:
                return

            rlist, wlist, _ = select.select(
                [gateway_connection._ssl],
                [gateway_connection._ssl],
                [],
                10)
            if rlist:
                try:
                    buff = gateway_connection.read(ERROR_RESPONSE_LENGTH)
                    if len(buff) == ERROR_RESPONSE_LENGTH:
                        command, status, error_identifier = \
                            unpack(ERROR_RESPONSE_FORMAT, buff)
                        if 8 == command:  # there is error response from APNS
                            found = False
                            while not pushed_buffer.empty():
                                identifier, job = pushed_buffer.get()
                                if found:
                                    self.send(job)
                                elif identifier == error_identifier:
                                    found = True
                except ssl.SSLError, e:
                    if e.errno == ssl.SSL_ERROR_SSL:
                        self.invalid = True
                except (socket.error, IOError), e:
                    pass
                gateway_connection.reconnect()
            elif wlist:
                job = self.push_queue.get()
                try:
                    push_id += 1
                    gateway_connection.send_notification(
                        job['device_token'],
                        Payload(**job['payload']),
                        push_id)
                    if pushed_buffer.full():
                        pushed_buffer.get()
                    pushed_buffer.put((push_id, job))
                    gevent.sleep(0.1)
                    continue
                except ssl.SSLError, e:
                    if e.errno == ssl.SSL_ERROR_SSL:
                        raise
                        self.invalid = True
                    gateway_connection.reconnect()
                except (socket.error, IOError), e:
                    gateway_connection.reconnect()

    def start(self, worker_count=1):
        for i in range(worker_count):
            self.threads.append(gevent.spawn(self.push_worker))

    def stop(self):
        for i in range(300):
            if self.push_queue.qsize():
                gevent.sleep(1)
            else:
                break

    def send(self, job):
        if self.invalid:
            return
        while self.push_queue.full():
            gevent.sleep(0.1)
        self.push_queue.put(job)
