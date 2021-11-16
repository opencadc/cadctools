"""
The cadcutils.net package contains network-related utilities commonly use in
CADC applications.

The main functions/classes are as follows (Note that they can be imported
directly from cadcutils.net):

    - get_cert: retrieves a X509 proxy certificate from the Credential
    Delegation Protocol (CDP)
                Web Service
    - Subject: container class for user credentials.
    - BaseWsClient: this is the base class for interacting with a Web service
    - GroupsClient: class used to interact with the CADC Group Management
    Service (GMS)

Note that the library also defines the entry_point for the cadc-get-cert at
cadcutils.net.auth:get_cert_main and one for cadc-groups at
cadcutils.net.groups_client.main_app

Examples of usage:
1. Instantiate Groups and use it to access GMS.

One mandatory argument that the CadcTapClient constructor takes is a
cadcutils.net.Subject that holds the user credentials.

Example:
   from cadcutils import net

   client = net.GroupsClient(net.Subject(certificate=<certificate-location>))
   print(client.get_group(<group_id>)
   ...
   # other client calls

2. Invoke the cadc-groups entry point function. This is the function that
is used to generate the cadc-groups application

Example:
   from cadcutils.net.groups_client import main_app
   import sys

   sys.argv = ['cadc-groups', '-h']
   main_app()

3. Invoke the cadc-groups as an external command
Example:
   import os
   os.system('cadc-groups --help')

Method 1. is the recommended method as it does not require forking external
process and also allows trapping the exceptions and reacting according to the
type of the error. Method 2 also works but the sys.exit needs to be trapped in
order to prevent the script from quiting. Method 3, while simple, must rely on
inter processes communication to determine the result of running the command.

"""

from .auth import *  # noqa
from .ws import *  # noqa
from .netutils import *  # noqa
from .group import *  # noqa
from .groups_client import *  # noqa
