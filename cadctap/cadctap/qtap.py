# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from cadctap.core import main_app as cadctap_main
import sys

def main_app():
    if len(sys.argv) > 1:
        if sys.argv[1] != 'query':
            sys.argv.insert(1, 'query')
    cadctap_main(command='qtap')
