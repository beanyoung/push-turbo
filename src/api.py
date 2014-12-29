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
    jobs = request.json
    if len(jobs) < 5:
        g.beanstalk.use(config.PUSH_TUBE)
        for job in jobs:
            priority = config.PRIORITIES.get(job.get('priority', 'low'))
            g.beanstalk.put(json.dumps(job), priority=priority)
    else:
        g.beanstalk.use(config.BATCH_PUSH_TUBE)
        g.beanstalk.put(json.dumps(jobs))
    return jsonify(dict())


@api.route('/push_stats', methods=['GET'])
def push_stats():
    g.beanstalk.watch('push')
    return jsonify(g.beanstalk.stats_tube(config.PUSH_TUBE))
