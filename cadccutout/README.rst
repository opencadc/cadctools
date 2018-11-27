cadccutout
===============
.. image:: https://img.shields.io/pypi/v/cadccutout.svg
    :target: https://pypi.python.org/pypi/cadcecutout

Cutout library written in Python that uses Astropy APIs.

Installation
------------

Can be installed using ``pip install cadccutout`` in Python 2.7 or 3.X.


API
---

Cutouts are performed unidirectionally, meaning the library assumes an
input stream that can only be read once, rather than seeking. If more
than one HDU was requested, then each HDU is iterated over and compared
from top to bottom. Single HDU requests are shortcircuited using the
Astropy ``astropy.io.fits.getdata()`` function

Python 3.x has a potential issue when appending to an output stream
where Astropy incorrectly sets a mode to prevent appending (See `Astropy
GitHub 7856`_).

Example 1
~~~~~~~~~

Perform a cutout of a file using the ``cfitsio`` cutout string format.

.. code:: python

       import tempfile
       from cadccutout import OpenCADCCutout

       test_subject = OpenCADCCutout()
       output_file = tempfile.mkstemp(suffix='.fits')
       input_file = '/path/to/file.fits'

       # Cutouts are in cfitsio format.
       cutout_region_string = '[300:800,810:1000]'  # HDU 0 along two axes.

       # Needs to have 'append' flag set.  The cutout() method will write out the data.
       with open(output_file, 'ab+') as output_writer, open(input_file, 'rb') as input_reader:
           test_subject.cutout(input_reader, output_writer, cutout_region_string, 'FITS')

Example 2 (CADC)
~~~~~~~~~~~~~~~~

Perform a cutout from an input stream from an HTTP request.

.. code:: python

       import tempfile
       from cadccutout import OpenCADCCutout
       from cadcdata import CadcDataClient

       test_subject = OpenCADCCutout()
       anonSubject = net.Subject()
       data_client = CadcDataClient(anonSubject)
       output_file = tempfile.mkstemp(suffix='.fits')
       archive = 'HST'
       file_name = 'n8i311hiq_raw.fits'
       input_stream = data_client.get_file(archive, file_name)

       # Cutouts are in cfitsio format.
       cutout_region_string = '[SCI,10][80:220,100:150]'  # SCI version 10, along two axes.

       # Needs to have 'append' flag set.  The cutout() method will write out the data.
       with open(output_file, 'ab+') as output_writer:
           test_subject.cutout(input_stream, output_writer, cutout_region_string, 'FITS')


Command Line Access
-------------------

The executable ``cadccutout`` is installed by default, or the module can be run using ``python -m cadccutout``.

Running
~~~~~~~

``cadccutout -d --infile path/to/source.fits --outfile path/to/output.fits [100:400]``

``cadccutout -d --infile path/to/source.fits --outfile path/to/output.fits "CIRCLE=10 60 0.5"``


Running in Docker
~~~~~~~~~~~~~~~~~

The provided `Dockerfile`_ can be used to build an image based on the desired
Python version.

Build an image for Python 2.7:

``docker build --build-arg PYTHON_VERSION=2.7 -t opencadc/cadccutout:2.7-alpine .``

Then execute it (``/usr/src/data`` is the location of the source files).  This will send the output to standard out:

``docker run --rm -v $(pwd):/usr/src/data opencadc/cadccutout:2.7-alpine cadccutout --infile /usr/src/data/myfile.fits [100:400]``

or

``docker run --rm --mount type=bind,source=$(pwd),target=/usr/src/data opencadc/cadccutout:2.7-alpine cadccutout --infile /usr/src/data/myfile.fits [100:400]``


Build an image for Python 3.6:

``docker build --build-arg PYTHON_VERSION=3.6 -t opencadc/cadccutout:3.6-alpine .``

Then execute it (``/usr/src/data`` is the location of the source files).  This will send the output to a FITS file:

``docker run --rm -v $(pwd):/usr/src/data opencadc/cadccutout:3.6-alpine cadccutout --infile /usr/src/data/myfile.fits --outfile /usr/src/data/mycutout_0_100_400.fits [100:400]``

or

``docker run --rm --mount type=bind,source=$(pwd,target=/usr/src/data opencadc/cadccutout:3.6-alpine cadccutout --infile /usr/src/data/myfile.fits --outfile /usr/src/data/mycutout_0_100_400.fits [100:400]``


Testing
-------

Docker
~~~~~~

The easiest thing to do is to run it with docker. OpenCADC has an
`AstroQuery docker image`_ available for runtime available in Python
2.7, 3.5, 3.6, and 3.7.

Run tests in Docker
^^^^^^^^^^^^^^^^^^^

You can mount the local dev directory to the image and run the python
test that way. From inside the dev (working) directory:

Python 3.7:

``docker run --rm -v $(pwd):/usr/src/app opencadc/astroquery:3.7-alpine python setup.py test``

or

``docker run --rm --mount type=bind,source=$(pwd),target=/usr/src/app opencadc/astroquery:3.7-alpine python setup.py test``

Python 2.7:

``docker run --rm -v $(pwd):/usr/src/app opencadc/astroquery:2.7-alpine python setup.py test``

or

``docker run --rm --mount type=bind,source=$(pwd),target=/usr/src/app opencadc/astroquery:2.7-alpine python setup.py test``

.. _Astropy GitHub 7856: https://github.com/astropy/astropy/pull/7856
.. _AstroQuery docker image: https://hub.docker.com/r/opencadc/astroquery/
.. _Dockerfile:  ./Dockerfile
