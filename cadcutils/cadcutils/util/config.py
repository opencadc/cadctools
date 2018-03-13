import errno
import logging
import os
import sys
from six.moves import configparser
from shutil import copyfile


logger = logging.getLogger('config')
logger.setLevel(logging.INFO)

if sys.version_info[1] > 6:
    logger.addHandler(logging.NullHandler())


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class Config(object):

    def __init__(self, config_path, default_config_path=None):
        logger.info("Using config file {0}.".format(config_path))

        # check config file exists and can be read
        if not os.path.isfile(config_path) and not os.access(config_path,
                                                             os.R_OK):
            error = "Can not read {0}.".format(config_path)
            logger.debug(error)
            raise IOError(error)

        self.parser = configparser.ConfigParser()
        if default_config_path:
            try:
                self.parser.readfp(open(default_config_path))
            except configparser.Error as exc:
                logger.debug("Error opening {0} because {1}.".format(
                    default_config_path, exc.message))

        try:
            self.parser.read(config_path)
        except configparser.Error as exc:
            logger.debug("Error opening {0} because {1}.".format(
                config_path, exc.message))

    def get(self, section, option):
        try:
            return self.parser.get(section, option)
        except (configparser.NoOptionError, configparser.NoSectionError):
            pass
        return None

    @staticmethod
    def write_config(config_path, default_config_path):
        """
        :param config_path:
        :param default_config_path:
        :return:
        """

        # if not local config file then write the default config file
        if not os.path.isfile(config_path):
            mkdir_p(os.path.dirname(config_path))
            copyfile(default_config_path, config_path)
            return

        # read local config file
        parser = configparser.ConfigParser()
        try:
            parser.read(config_path)
        except configparser.Error as exc:
            logger.debug("Error opening {0} because {1}.".format(config_path,
                                                                 exc.message))
            return

        # read default config file
        default_parser = configparser.RawConfigParser()
        try:
            default_parser.read(default_config_path)
        except configparser.Error as exc:
            logger.debug("Error opening {0} because {1}.".format(
                default_config_path, exc.message))
            return

        # update config file with new options from the default config
        updated = False
        for section in default_parser.sections():
            default_items = default_parser.items(section)
            for option, value in default_items:
                if not parser.has_section(section):
                    parser.add_section(section)
                if not parser.has_option(section, option):
                    parser.set(section, option, value)
                    updated = True

        # remove old options not in the default config file?
        # for section in default_parser.sections():
        #     options = parser.options(section)
        #     for option in options:
        #         if not default_parser.has_option(section, option):
        #             parser.remove_option(section, option)

        # write updated config file
        if updated:
            try:
                config_file = open(config_path, 'w')
                parser.write(config_file)
                config_file.close()
            except Exception as exc:
                print(exc.message)
