
# OpenCADC Python Contribution Guidlines

### General Guidelines
OpenCADC Python software is hosted by GitHub (GH). In general, there are repos containing a number of application projects. An project might be published on `PyPI` (The Python Project Index).
The OpenCADC Python software is expected to be available and work with all the released supported versions of Python (https://devguide.python.org/versions/) 

### Directory Structure
A project, let's say `myapp` has the following structure:
`myapp` root directory with the following files:
* `setup.py` - script used by `pip` command to run install the application
* `setup.cfg` - configuration file for `pip`. It contains the dependencies and the version of the application.
* `README.md` - description of the project
* `LICENCE` - the license file
* `tox.ini` - `tox` configuration file

Subdirectories:
* `myapp/myapp` - contains the Python source code and the unit test code. The directory can contain multiple subdirectories. Each directory might have a `tests` directory for the unit tests corresponding to the directory. `tests` directories might have `data` subdirectories containing the test data used in unit tests.


### Development tools
Contributions to the OpenCADC Python projects is expected to be submitted as Pull Requests (PR) from other forks of the project to the default branch (`main` or `master`).

Required Python packages:
- `pytest` - used to run the unit tests
- `flake8` - used to check the conformance with the coding styles. The CADC coding styles are very similar to the default `flake8` styles.
- `tox` - used to setup virtual environments with different configurations and run tests
- `virtualenv` - (optional) to create a custom virtual environment
- `coverage` - (optional) the coverage Python package is used to measure the code coverage achieved through the unit tests

The code should follow the standard [PEP8 Style Guide for Python Code](https://peps.python.org/pep-0008/). In particular, this includes using only 4 spaces for indentation, and never tabs.

Testing:
To run all the available configuration, run the 
```commandline
tox
```

To check the available `tox` targets:
```commandline
tox list
```

`tox` targets are created in the `.tox` subdirectory. To activate a specific virtual environment:
```commandline
source .tox/py310/bin/activate
```

Once the virtual environment has been activated, use `pip` to install required software. For ex:
```commandline
pip install -e ".[test]"
```
to install the test environment.

To run the unit tests from a virtual environment:
```commandline
pytest <package_name>
```

### GitHub, Continuous Integration/ Continous Development (CI/CD)
In order for a contribution (PR) to be merged into the project branch, it needs to be reviwed and accepted by at least one of the package maintainers and pass the CI/CD pre-conditions. These are implemented as GH Actions and check the following:
- project `egg` builds successfully
- unit tests are successfully executed in all the supported configurations (Python versions)
- no code style errors are present
- test code coverage does not decrease

CADC staff can also run integration tests. These are not always accessible to external collaborators because they require Authentication/Authorization (A&A).

Once the a patch has been merged into the default branch it can, depending on the urgency and its content, be published to `PyPI` immediately as an updated version or it can be bunddled with other minor changes to be released later. This is at the discretion of the package maintainers.




