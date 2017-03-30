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
    """
    Class to read and write a configuration file.
    """

    def __init__(self, config_file, default_config_file=None):
        """
        Init the Config class.
        If config_file is not found, or cannot be read, raises an IOError.
        If default_config_file is given, read in the default options. Then
        read config_file overriding any default options if found.
        
        :param config_file: absolute path to the local configuration file
        :param default_config_file: absolute path to the default configuration file
        """
        logger.info("Using config file {0}.".format(config_file))

        # check config file exists and can be read
        if not os.path.isfile(config_file) and not os.access(config_file, os.R_OK):
            error = "Can not read configuration file {0}.".format(config_file)
            logger.error(error)
            raise IOError(error)

        self.parser = configparser.ConfigParser()
        if default_config_file:
            try:
                self.parser.readfp(open(default_config_file))
            except configparser.Error as exc:
                logger.error("Error opening {0} because {1}.".format(default_config_file, exc.message))

        try:
            self.parser.read(config_file)
        except configparser.Error as exc:
            logger.error("Error opening {0} because {1}.".format(config_file, exc.message))

    def get(self, section, option):
        """
        Get the value of the given option in the given section.
        
        :param section: section name
        :param option: option name
        :return: the option value in the section, or a NoSectionError if the section is not found,
        or a NoOptionError if the option is not found in the section.
        """
        return self.parser.get(section, option)

    @staticmethod
    def write_config(config_file, default_config_file):
        """
        Writes a new configuration file, or updates an existing one.
        If config_file is not found, default_config_file is copied to config_file.
        If config_file exists, any new sections and options are copied from default_config_file
        to config_file. Existing options in config_file are not overwritten, nor are
        options deleted from config_file that do not exist in default_config_file.
        
        :param config_file: absolute path to the local configuration file
        :param default_config_file: absolute path to the default configuration file
        :return
        """

        # if not local config file then write the default config file
        if not os.path.isfile(config_file):
            mkdir_p(os.path.dirname(config_file))
            copyfile(default_config_file, config_file)
            return

        # read local config file
        parser = configparser.ConfigParser()
        try:
            parser.read(config_file)
        except configparser.Error as exc:
            logger.error("Error opening {0} because {1}.".format(config_file, exc.message))
            return

        # read default config file
        default_parser = configparser.ConfigParser()
        try:
            default_parser.read(default_config_file)
        except configparser.Error as exc:
            logger.error("Error opening {0} because {1}.".format(default_config_file, exc.message))
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

        # write updated config file
        if updated:
            try:
                config_file = open(config_file, 'w')
                parser.write(config_file)
                config_file.close()
            except Exception as exc:
                print (exc.message)
