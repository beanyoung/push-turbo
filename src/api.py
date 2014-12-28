#!/usr/bin/python
# -*- coding: utf-8 -*-

import json

import beanstalkc
from flask import Blueprint, jsonify, g, request

import config


api = Blueprint('api', __name__)


@api.before_request
def before_request():
    g.beanstalk = beanstalkc.Connection(
        host=config.BEANSTALKD_HOST, port=config.BEANSTALKD_PORT)


@api.route('/push', methods=['POST'])
def push_jobs():
    job = dict()
    job['app_name'] = request.form.get('app_name')
    job['payload'] = dict()
    job['payload']['alert'] = request.form.get('alert')
    job['payload']['sound'] = request.form.get('sound', 'default')
    job['payload']['badge'] = int(request.form.get('badge', '1'))
    job['payload']['custom'] = json.loads(request.form.get('custom', '{}'))

    device_tokens = request.form.get('device_tokens').split(',')
    if len(device_tokens) < 5:
        g.beanstalk.use('push')
        for device_token in device_tokens:
            job['device_token'] = device_token
            g.beanstalk.put(json.dumps(job))
    else:
        g.beanstalk.use('batch_push')
        job['device_tokens'] = device_tokens
        g.beanstalk.put(json.dumps(job))
    return jsonify(dict())


@api.route('/push_stats', methods=['GET'])
def push_stats():
    g.beanstalk.watch('push')
    return jsonify(g.beanstalk.stats_tube('push'))
