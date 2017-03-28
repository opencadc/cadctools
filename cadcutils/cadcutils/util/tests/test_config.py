from six.moves import configparser
import os
import tempfile
import shutil
import uuid
import unittest2 as unittest
from cadcutils.util import config

_ROOT = os.path.abspath(os.path.dirname(__file__))


def get_test_data_file_path(filename):
    return os.path.join(_ROOT, 'data', filename)


class TestConfig(unittest.TestCase):
    """Test the vos Config class.
    """

    def test_single_section_config(self):

        self.do_test('single-section-config', 'single-section-default-config')

    def test_multi_section_config(self):

        self.do_test('multi-section-config', 'multi-section-default-config')

    def do_test(self, config_filename, default_config_filename):

        default_config_path = tempfile.gettempdir() + '/' + str(uuid.uuid4())
        test_default_config = get_test_data_file_path(default_config_filename)
        shutil.copy(test_default_config, default_config_path)

        # no existing config file
        config_path = tempfile.gettempdir() + '/' + str(uuid.uuid4())

        config.Config.write_config(config_path, default_config_path)

        self.assertTrue(os.path.isfile(config_path))
        self.cmp_configs(config_path, default_config_path)

        # existing config file same as default config file
        config_path = tempfile.gettempdir() + '/' + str(uuid.uuid4())
        shutil.copy(test_default_config, config_path)

        config.Config.write_config(config_path, default_config_path)

        self.cmp_configs(config_path, default_config_path)

        # merge default and existing config files
        config_path = tempfile.gettempdir() + '/' + str(uuid.uuid4())

        test_config = get_test_data_file_path(config_filename)
        shutil.copy(test_config, config_path)

        config.Config.write_config(config_path, default_config_path)

        self.cmp_configs(config_path, default_config_path)

    def cmp_configs(self, config_path, default_config_path):

        parser = configparser.ConfigParser()
        parser.read(config_path)

        default_parser = configparser.ConfigParser()
        default_parser.read(default_config_path)

        for section in default_parser.sections():
            self.assertTrue(parser.has_section(section))
            for option in default_parser.options(section):
                self.assertTrue(parser.has_option(section, option))
