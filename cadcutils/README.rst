cadcutils
=========

.. image:: https://img.shields.io/pypi/v/cadcutils.svg   
    :target: https://pypi.python.org/pypi/cadcutils

Canadian Astronomy Data Centre (CADC) library of utilities commonly used
by CADC Python clients

Command Line Interface API
==========================
This library installs a number of CLI applications. Please consult their
helpers for details on how to invoke them.

cadc-get-cert
-------------
Application that retrieves a CADC X509 user proxy certificate. This
certificate is used to authenticate with a number of CADC clients.

cadc-groups
-----------
Application that interacts with the CADC Group Management Service (GMS).
GMS is used to check user membership in appropriate groups in order to
determine authorization access to resources. The application is used
to manipulate CADC groups.



