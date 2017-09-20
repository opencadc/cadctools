
# Licensed under a 3-clause BSD style license - see LICENSE.rst

"""
This package implements a client for access the CADC data Web service (WS)

The package can be used as a library as well as through the cadc-data
command it installs.

1. Instantiate CadcDataClient and use it to access the data WS.

The only mandatory argument that the CadcDataClient constructor takes is
a cadcutils.net.Subject that holds the user credentials. The data WS is
accessed through the get_file, put_file and get_file_info functions of the
client.

Example:
   from cadcdata import CadcDataClient
   from cadcutils import net

   client = CadcDataClient(net.Subject())
   print(client.get_file_info('GEMINI', '00AUG02_002'))

2. Invoke the cadc-data entry point function. This is the function that
is used to generate the cadc-data application

Example:
   from cadcdata import main_app
   import sys

   sys.argv = ['cadc-data', 'info', '-a', 'GEMINI', '00AUG02_002']
   main_app()

3. Invoke the cadc-data as an external command
Example:
   import os
   os.system('cadc-data info -a GEMINI 00AUG02_002')

Method 1. is the recommended method as it does not required forking external
processes and also allows trapping the exceptions and reacting according to the
type of the error. Method 2 also works but the sys.exit needs to be trapped in
order to prevent the script from quiting. Method 3, while simple, must rely on
inter processes communication to determine the result of running the command.

"""

from .core import *   # noqa
