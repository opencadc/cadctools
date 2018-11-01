# Integration Testing

The integration tests here ONLY run within the CADC, currently.  These tests rely on specific cubes for their size,
complexity, etc. for accurate archive testing.  Some of the files are also proprietary and cannot be accessed without
proper credentials.

The rest, however, could be obtained through the CADC's data web service on demand, but would add significant download
time.

## Running

The tests rely on a `/usr/src/data` folder to contain the original FITS file to be cutout from, as well as a cutout file to compare to, and is expected to contain files for each of these archives:


| Archive Name | Source file name | Expected cutout file name |
| ------------ | ---------------- | ------------------------- |
|VLASS|test-vlass-cube.fits|test-vlass-cube-cutout.fits|
|CGPS|test-cgps-cube.fits|test-cgps-cube-cutout.fits|
|ALMA|test-alma-cube.fits|test-alma-cube-cutout.fits|
|GMIMS (Currently only proprietary)|test-gmims-cube.fits|test-gmims-cube-cutout.fits|
|SITELLE (CFHT)|test-sitelle-cube.fits|test-sitelle-cube-cutout.fits|
|MAST (HST)|test-hst-mef.fits|test-hst-mef-cutout.fits|


### Docker

Docker is the easiest way to run the integration tests.  The `/usr/src/data` folder should be mounted as a volume.

Ideally, these would be downloaded as needed, but would need a lot of extra time to download each of these.  The VLASS cube, for example, is 76GB in size.


#### Example (from the project folder)

##### Python 2.7
```shell
$ cd /path/to/opencadc_cutout/project
$ docker run --rm -t -v $(pwd):/usr/src/app -v /path/to/data:/usr/src/data opencadc/astroquery:2.7-alpine python setup.py int_test
```

##### Python 3.7 (Astropy bug prevents 3.x for now.  See [Astropy GitHub 7856](https://github.com/astropy/astropy/pull/7856))
```shell
$ cd /path/to/opencadc_cutout/project
$ docker run --rm -t -v $(pwd):/usr/src/app -v /path/to/data:/usr/src/data opencadc/astroquery:3.7-alpine python setup.py int_test
```
