#!/usr/bin/python
# -*- coding: utf-8 -*-

import json

import beanstalkc
from flask import Blueprint, current_app, jsonify, g, request

import config


api = Blueprint('api', __name__)


@api.before_request
def before_request():
    g.beanstalk = beanstalkc.Connection(
        host=config.BEANSTALKD_HOST, port=config.BEANSTALKD_PORT)


@api.route('/push', methods=['POST'])
def push_jobs():
    jobs = request.json
    for job in jobs:
        if job['app_name'] not in config.APPS:
            ret = dict(
                error='unknown_app_name',
                detail='Unknown app name %s' % job['app_name'])
            return jsonify(ret), 400

    if len(jobs) < 5:
        for job in jobs:
            if job['app_name'] not in config.APPS:
                continue
            g.beanstalk.use(config.PUSH_TUBE % job['app_name'])
            priority = config.PRIORITIES.get(job.get('priority', 'low'))
            delay = job.get('delay', 0)
            g.beanstalk.put(json.dumps(job), priority=priority, delay=delay)
    else:
        g.beanstalk.use(config.BATCH_PUSH_TUBE)
        g.beanstalk.put(json.dumps(jobs))
    current_app.logger.info(jobs)
    return jsonify(dict())


@api.route('/push_stats', methods=['GET'])
def push_stats():
    ret = g.beanstalk.stats()
    ret['tubes'] = []
    for app_name in config.APPS.keys():
        ret['tubes'].append(
            g.beanstalk.stats_tube(config.PUSH_TUBE % app_name))
    return jsonify(ret)
