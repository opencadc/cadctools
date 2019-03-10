cadctools
=========

.. image:: https://img.shields.io/pypi/pyversions/cadcutils.svg
    :target: https://pypi.python.org/pypi/cadcutils

.. image:: https://img.shields.io/travis/opencadc/cadctools/master.svg
    :target: https://travis-ci.org/opencadc/cadctools?branch=master

.. image:: https://img.shields.io/coveralls/opencadc/cadctools/master.svg
    :target: https://coveralls.io/github/opencadc/cadctools?branch=master

.. image:: https://img.shields.io/github/contributors/opencadc/cadctools.svg
    :target: https://github.com/opencadc/cadctools/graphs/contributors



Client tools and utilities for using Canadian Astronomy Data Centre services


Developers Guide
================


Requires pip.

Installing Packages
-------------------

::

    cd cadcutils && pip install -r ./dev_requirements.txt
    cd cadcdata && pip install -r ./dev_requirements.txt
    cd cadcetrans && pip install -r ./dev_requirements.txt
    cd cadccutout && pip install -r ./dev_requirements.txt

Testing packages
----------------

Testing cadcutils
~~~~~~~~~~~~~~~~~

::

    cd ./cadcutils
    python setup.py test

Testing cadcdata
~~~~~~~~~~~~~~~~

::

    cd ./cadcdata
    python setup.py test

Testing cadccutout
~~~~~~~~~~~~~~~~

::

    cd ./cadccutout
    python setup.py test


Checkstyle
~~~~~~~~~~
flake8 style checking is enforced on pull requests. Following commands should
not report errors

::

     flake8 cadcutils/cadcutils cadcdata/cadcdata cadcetrans/cadcetrans
     cadccutout/cadccutout


Usage Example
-------------

In library mode
~~~~~~~~~~~~~~~

Write the following into a file named ``test.py``

::

    from cadcdata import CadcDataClient
    from cadcutils import net

    client = CadcDataClient(net.Subject())
    print(client.get_file_info('GEMINI', '00AUG02_002'))

Then Run

::

    python test.py

Direct Usage
~~~~~~~~~~~~

After installing the cadcdata package, run

::

    cadc-data get GEMINI 00AUG02_002

This will download the fits file to your current directory.

To see more information do

::

    cadc-data put --help
    cadc-data get --help
    cadc-data info --help

Docker image
------------

Rather than deploying a project environment, you could just use docker.

To use

1. Install docker.

2. Then run:

   ::

       docker build . -t cadc/cadctools
       docker run --name cadctools cadc/cadctools 
