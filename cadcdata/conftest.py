# content of conftest.py

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--intTest", action="store_true", default=False, help="run integration tests"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--intTest"):
        # --intTest given in cli: do not skip intTest tests
        return
    skip_int_test = pytest.mark.skip(reason="need --intTest option to run")
    for item in items:
        if "intTest" in item.keywords:
            item.add_marker(skip_int_test)
