cadctools
=========

.. image:: https://img.shields.io/pypi/pyversions/cadcutils.svg
    :target: https://pypi.python.org/pypi/cadcutils

.. image:: https://github.com/opencadc/cadctools/workflows/CI/badge.svg?branch=master&event=schedule

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
    cd cadctap && pip install -r ./dev_requirements.txt
    cd cadccutout && pip install -r ./dev_requirements.txt

Testing packages
----------------

Testing cadcutils
~~~~~~~~~~~~~~~~~

::

    cd ./cadcutils
    pytest cadcutils

Testing cadcdata
~~~~~~~~~~~~~~~~

::

    cd ./cadcdata
    python cadcdata

Testing cadctap
~~~~~~~~~~~~~~~~

::

    cd ./cadcdata
    python cadctap

Testing cadccutout
~~~~~~~~~~~~~~~~

::

    cd ./cadccutout
    pytest cadccutout


Checkstyle
~~~~~~~~~~
flake8 style checking is enforced on pull requests. Following commands should
not report errors

::

     flake8 cadcutils/cadcutils cadcdata/cadcdata cadcetrans/cadcetrans
     cadccutout/cadccutout


Testing with tox
~~~~~~~~~~~~~~~~

If tox, the generic virtual environment tool, is available it can be used to test with different versions of
python is isolation. For example, to test on all supported versions of Python in cadcdata (assuming that
they are available in the system):

::

    cd ./cadcdata && tox

To test a specific version:

::

    cd ./cadcdata && tox -e py3.7


To list all the available environments:

::

    cd ./cadcdata && tox -a


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
