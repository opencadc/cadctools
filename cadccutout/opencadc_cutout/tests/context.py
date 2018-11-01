# -*- coding: utf-8 -*-

import sys
import os
import random
import string
import tempfile

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

import opencadc_cutout


TEST_FILE_DIR='/tmp'
def random_test_file_name_path(file_extension='fits', dir_name=TEST_FILE_DIR):
    return tempfile.NamedTemporaryFile(dir=dir_name, prefix=__name__, suffix='.{}'.format(file_extension)).name
