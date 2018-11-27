cadcetrans
==========

.. image:: https://img.shields.io/pypi/v/cadcetrans.svg   
    :target: https://pypi.python.org/pypi/cadcetrans

Canadian Astronomy Data Centre - electronic transfer
----------------------------------------------------

``cadc-etrans`` is an application for electronic transfer of data and
metadata to the Canadian Astronomy Data Centre (CADC). Files to be
processed by ``cadc-etrans`` are to be placed in one of the following
subdirectories of a source directory specified by the user:

- new - for files expected to be new to the CADC archive. ``cadc-etrans``
  flags as errors when this is not the case.
- replaced - for files expected to be in the CADC archive already.
  ``cadc-etrans`` flags it as an error if the files are missing.
- any - for files whose presence in the CADC archive is not important

``cadc-etrans`` can be configured to perform checks on the names of the files
according to provided rules and checks on the type of the file. Files that
fail these checks are moved to a rejected subdirectory and grouped according
to the type of the encountered error. Users are expected to fix the problems
and placed the files back in the source directory for reprocessing.

Files that pass all the verifications are sent to the CADC archive.

NOTE: To ensure that a file is fully received before attempting to transfer
it, it must spend a minimum amount of time (5min) in the input directory
without being modified/updated prior to its processing.

Functionality of ``cadc-etrans`` is configured by modifying the
``~/.config/cadc-etrans`` file.

Usage
-----

``cadc-etrans`` is usually used with a crontab. Example below processes files
every 15 min.

::

    */15 * * * * cadc-etrans data --cert /home/auser/.ssl/cadcproxy.pem
    -c /home/auser/.config/cadc/dao-namecheck.xml sourcedir

``cadc-etrans`` can backup the transfer logs to a vospace:

::

    0 11 * * * cadc-etrans status -b --cert /home/auser/.ssl/cadcproxy.pem
    sourcedir


Docker Usage
------------

To avoid deploying the application environment (e.g. installing fitsverify on
the host), the application can be run from a docker container.
To build the container, download the content of the
`https://github.com/opencadc/cadctools/tree/master/cadcetrans/docker` directory
customize the config files ``cadc-etrans-config`` and ``namecheck.xml`` and
build the container:

::

    cd docker
    docker build -t cadcetrans .

To invoke it:

::

    docker run --rm --mount type=bind,source=/tmp/logs,target=/logging
    --mount type=bind,source=/tmp/input/,target=/input cadcetrans status

or:

::

    docker run --rm --mount type=bind,source=/tmp/logs,target=/logging
    --mount type=bind,source=/tmp/input/,target=/input cadcetrans data

Note the two mounts that are required: one where the transfer logs will be
recorded so that they are available outside the container and the other
one for the source directory (``source=<>`` part is what needs to be customized
to point to directories on the local host.
