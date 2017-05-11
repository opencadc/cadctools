# cadctools
Client tools for using CADC services

Build status:
<a href="https://travis-ci.org/opencadc/cadctools"><img src="https://travis-ci.org/opencadc/cadctools.svg?branch=master" /></a>

# Developers Guide

Requires Python version: 2.7, 3.3, 3.4, 3.5.

Requires pip.


## Installing Packages

Run 
```
cd cadcutils && pip install -r ./dev_requirements.txt
cd cadcdata && pip install -r ./dev_requirements.txt
```

## Testing packages

### Testing cadcutils

```
cd ./cadcutils
python setup.py test
```

### Testing cadcdata

```
cd ./cadcdata
python setup.py test
```

## Usage Example

### In library mode

Write the following into a file named `test.py`
```
from cadcdata import CadcDataClient
from cadcutils import net

client = CadcDataClient(net.Subject())
print(client.get_file_info('GEMINI', '00AUG02_002'))
```

Then Run
```
python test.py
```

### Direct Usage

After installing the cadcdata package, run 
```
cadc-data get GEMINI 00AUG02_002
```

This will download the fits file to your current directory. 

To see more information do

```
cadc-data put --help
cadc-data get --help
cadc-data info --help
```

## Docker image

Rather than deploying a project environment, you could just use docker. 

To use

1. Install docker. 

2. Then run: 
```
docker build . -t cadc/cadctools
docker run --name cadctools cadc/cadctools 
```

