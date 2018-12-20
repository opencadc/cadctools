
# Licensed under a 3-clause BSD style license - see LICENSE.rst

"""
This package implements a client for access the CADC TAP Web service (WS)

The package can be used as a library as well as through the cadc-tap
command it installs.

1. Instantiate YoucatClient and use it to access the data WS.

The only mandatory argument that the YoucatClient constructor takes is
a cadcutils.net.Subject that holds the user credentials. The data WS is
accessed in each of the functions in the client.

Example:
   from cadctap.youcat import YoucatClient
   from cadcutils import net

   client = YoucatClient(net.Subject())
   print(client.schema())

2. Invoke the cadc-tap entry point function. This is the function that
is used to generate the cadc-tap application

Example:
   from cadctap.core import main_app
   import sys

   sys.argv = ['cadc-tap', 'schema']
   main_app()

3. Invoke the cadc-tap as an external command
Example:
   import os
   os.system('cadc-tap schema')

Method 1. is the recommended method as it does not required forking external
processes and also allows trapping the exceptions and reacting according to the
type of the error. Method 2 also works but the sys.exit needs to be trapped in
order to prevent the script from quiting. Method 3, while simple, must rely on
inter processes communication to determine the result of running the command.

"""

from .core import *   # noqa
