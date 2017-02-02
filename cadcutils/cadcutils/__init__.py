# Licensed under a 3-clause BSD style license - see LICENSE.rst

"""
The cadcutils package contains libraries commonly use in CADC applications.

The main modules and packages in cadcutils are:

    -utils: collection of utilities for time formats, application logging
            and command line argument parsing
    -net: network utilities: authentication, Web service base client, etc
    -exceptions: exceptions thrown by the cadcutils libraries

"""

# Affiliated packages may add whatever they like to this file, but
# should keep this content at the top.
# ----------------------------------------------------------------------------
from ._astropy_init import *
# ----------------------------------------------------------------------------

# For egg_info test builds to pass, put package imports here.
if not _ASTROPY_SETUP_:
#    from .example_mod import *
    pass

