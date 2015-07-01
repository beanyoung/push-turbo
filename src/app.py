#!/usr/bin/python
# -*- coding: utf-8 -*-

from flask import Flask

import api
import config


app = Flask(__name__)
app.config.from_object('config')

app.register_blueprint(api.api, url_prefix='/api')

app.logger.setLevel(config.LOGGING_LEVEL)
for log_handler in config.LOGGING_HANDLERS:
    app.logger.addHandler(log_handler)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
