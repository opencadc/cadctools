# -*- coding: utf-8 -*-

import sys
import os
import random
import string
import tempfile
import vos

from cadcdata import CadcDataClient
from cadcutils import net


sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

import opencadc_cutout
from opencadc_cutout.tests.context import *

def get_file(archive, file_name, cutout=None, destination=None):
    anonSubject = net.Subject()
    data_client = CadcDataClient(anonSubject)
    return data_client.get_file(archive, file_name, cutout=cutout, destination=destination)

def get_vos_file(vos_uri, cutout=None):
    vos_client = vos.Client()
    if cutout is None:
        view = 'data'
    else:
        view = 'cutout'

    return vos_client.open(vos_uri, view=view, cutout=cutout)
