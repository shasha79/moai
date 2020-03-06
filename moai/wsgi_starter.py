import os
from configparser import ConfigParser

from wsgi import app_factory

config_file = os.environ['FEED_CONFIG']
config = ConfigParser()
config.read(config_file)
conf = dict(config['app:jhn'].items())
app = app_factory(None, **conf)
