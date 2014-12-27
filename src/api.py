#!/usr/bin/python
# -*- coding: utf-8 -*-

import beanstalkc
from flask import Blueprint, jsonify, g

import config


api = Blueprint('api', __name__)


@api.before_request
def before_request():
    g.beanstalk = beanstalkc.Connection(
        host=config.BEANSTALKD_HOST, port=config.BEANSTALKD_PORT)
    g.beanstalk.watch('push')


@api.route('/push', methods=['POST'])
def push_jobs():
    pass


@api.route('/push_stats', methods=['GET'])
def push_stats():
    return jsonify(g.beanstalk.stats_tube('push'))
