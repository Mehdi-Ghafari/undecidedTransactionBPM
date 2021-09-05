# coding: utf-8
import errno
import logging
import datetime
import os


class Logger(object):

    def __init__(self, filename: object = "", level: object = logging.DEBUG, dirname: object = "",
                 rootdir: object = "") -> object:

        now = datetime.datetime.now()

        self.logger = logging.getLogger(filename)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        self.logger.setLevel(level)

        try:
            if not os.path.exists(rootdir + '/' + now.strftime("%Y-%m-%d") + '/'):
                os.makedirs(rootdir + '/' + now.strftime("%Y-%m-%d") + '/')

            fh = logging.FileHandler(
                rootdir + '/' + now.strftime("%Y-%m-%d") + '/' + dirname + ',' + '%s' % filename + '.log', 'a')

            fh.setFormatter(formatter)

            # sh = logging.StreamHandler()
            self.logger.addHandler(fh)

        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    def debug(self, msg):
        self.logger.debug(str(msg))

    def info(self, msg):
        self.logger.info(str(msg))

    def warning(self, msg):
        self.logger.warning(str(msg))

    def error(self, msg):
        self.logger.error(str(msg))
