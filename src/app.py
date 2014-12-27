#!/usr/bin/python
# -*- coding: utf-8 -*-

from flask import Flask

import api


app = Flask(__name__)
app.config.from_object('config')

app.register_blueprint(api.api, url_prefix='/api')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
