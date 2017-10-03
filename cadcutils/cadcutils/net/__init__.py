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

Note that the library also defines and entry_point for the cadc-get-cert at
net.auth:get_cert_main


"""

from .auth import *  # noqa
from .ws import *  # noqa
