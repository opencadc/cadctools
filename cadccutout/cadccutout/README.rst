opencadc_cutout
===============

Cutout library written in Python that uses Astropy APIs.

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
       from opencadc_cutout import OpenCADCCutout

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
       from opencadc_cutout import OpenCADCCutout
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

``docker run --rm -v $(pwd):/usr/src/app opencadc/astroquery:3.7-alpine python setup.py test``

``docker run --rm -v $(pwd):/usr/src/app opencadc/astroquery:2.7-alpine python setup.py test``

.. _Astropy GitHub 7856: https://github.com/astropy/astropy/pull/7856
.. _AstroQuery docker image: https://hub.docker.com/r/opencadc/astroquery/
