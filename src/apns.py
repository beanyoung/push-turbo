# PyAPNs was developed by Simon Whitaker <simon@goosoftware.co.uk>
# Source available at https://github.com/simonwhitaker/PyAPNs
#
# PyAPNs is distributed under the terms of the MIT license.
#
# Copyright (c) 2011 Goo Software Ltd
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from binascii import a2b_hex
import json
import logging
from struct import pack, unpack

from gevent import ssl, socket


ENHANCED_NOTIFICATION_COMMAND = 1
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
MAX_PAYLOAD_LENGTH = 2048
TOKEN_LENGTH = 32
ERROR_RESPONSE_LENGTH = 6
ERROR_RESPONSE_FORMAT = (
    '!'  # network big-endian
    'B'  # command
    'B'  # status
    'I'  # identifier
)


class APNsConnection(object):
    """
    A generic connection class for communicating with the APNs
    """
    def __init__(self, cert_file=None, key_file=None):
        super(APNsConnection, self).__init__()
        self.cert_file = cert_file
        self.key_file = key_file
        self._socket = None
        self._ssl = None
        self.connection_alive = False

    def __del__(self):
        self.disconnect()

    def connect(self):
        logging.debug('Connect to apns start')
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        logging.debug('Connect to apns step 1')
        self._socket.connect((self.server, self.port))
        logging.debug('Connect to apns step 2')
        self._ssl = ssl.wrap_socket(
            self._socket,
            self.key_file,
            self.cert_file)
        logging.debug('Connect to apns step 3')
        self.connection_alive = True
        logging.debug('Connect to apns end')

    def disconnect(self):
        logging.debug('Disonnect from apns start')
        if self.connection_alive:
            if self._socket:
                self._socket.close()
            if self._ssl:
                self._ssl.close()
            self.connection_alive = False
        logging.debug('Disonnect from apns end')

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


class InvalidTokenError(StandardError):
    def __init__(self, token_hex):
        super(InvalidTokenError, self).__init__()
        self.token_hex = token_hex


class GatewayConnection(APNsConnection):
    def __init__(self, host, port, **kwargs):
        super(GatewayConnection, self).__init__(**kwargs)
        self.server = host
        self.port = port

    def get_notification(self, token_hex, payload, identifier, expiry):
        try:
            token = a2b_hex(token_hex)
        except TypeError:
            raise InvalidTokenError(token_hex)
        payload = payload.json()
        fmt = ENHANCED_NOTIFICATION_FORMAT % len(payload)
        notification = pack(
            fmt, ENHANCED_NOTIFICATION_COMMAND, identifier,
            expiry, TOKEN_LENGTH, token, len(payload), payload)
        return notification

    def send_notification(self, token_hex, payload, identifier=0, expiry=0):
        self.write(
            self.get_notification(token_hex, payload, identifier, expiry))
