
# Licensed under a 3-clause BSD style license - see LICENSE.rst

"""
This package implements a client for access the CADC Storage Inventory Web
services (SI)

The package can be used as a library as well as through the
cadc[get|put|info|remove] commands it installs.

1. Instantiate StorageInventoryClient and use it to access the SI WS.

The only mandatory argument that the StorageInventoryClient constructor takes
is a cadcutils.net.Subject that holds the user credentials. The SI WS is
accessed through the cadcget, cadcput, cadcinfo and cadcremove functions of the
client.

Example:
   from cadcdata import StorageInventoryClient
   from cadcutils import net

   client = StorageInventoryClient(net.Subject())
   print(client.cadcinfo('gemini:GEMINI/00AUG02_002.fits'))

2. Invoke the cadc* entry point functions. These are the functions that
is used to generate the cadc-data application

Example:
   from cadcdata import cadcinfo_cli
   import sys

   sys.argv = ['cadcinfo', 'gemini:GEMINI/00AUG02_002.fits']
   cadcinfo_cli()

3. Invoke cadc* as external OS command
Example:
   import os
   os.system('cadcget gemini:GEMINI/00AUG02_002.fits')

Method 1. is the recommended method as it does not required forking external
processes and also allows trapping the exceptions and reacting according to the
type of the error. Method 2 also works but the sys.exit needs to be trapped in
order to prevent the script from quiting. Method 3, while simple, must rely on
inter processes communication to determine the result of running the command.

"""

from .core import *   # noqa
#from .storageinv import *   # noqa
