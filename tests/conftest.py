"""R2X pytest fixtures.

Here goes all the variables that will be shared between all the different testing scripts.
"""

import pytest
from r2x.utils import read_json
from loguru import logger
from _pytest.logging import LogCaptureFixture
from tests.models.pjm import pjm_2area


DATA_FOLDER = "tests/data"
OUTPUT_FOLDER = "r2x_output"
DEFAULT_SCENARIO = "pacific"
DEFAULT_INFRASYS = "pjm_2area"


@pytest.fixture
def data_folder(pytestconfig):
    return pytestconfig.rootpath.joinpath(DATA_FOLDER)


@pytest.fixture
def tmp_folder(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp(OUTPUT_FOLDER)
    yield tmp_path


@pytest.fixture
def reeds_data_folder(pytestconfig):
    return pytestconfig.rootpath.joinpath(DATA_FOLDER).joinpath(DEFAULT_SCENARIO)


@pytest.fixture
def infrasys_data_folder(pytestconfig):
    return pytestconfig.rootpath.joinpath(DATA_FOLDER).joinpath(DEFAULT_INFRASYS)


@pytest.fixture
def default_scenario() -> str:
    return DEFAULT_SCENARIO


@pytest.fixture
def defaults_dict() -> dict[str, str]:
    config_dict = read_json("r2x/defaults/config.json")
    plugins_dict = read_json("r2x/defaults/plugins_config.json")
    plexos_dict = read_json("r2x/defaults/plexos_output.json")
    sienna_dict = read_json("r2x/defaults/sienna_config.json")
    combined_dict = config_dict | plugins_dict | plexos_dict | sienna_dict
    return combined_dict


@pytest.fixture
def caplog(caplog: LogCaptureFixture):
    handler_id = logger.add(
        caplog.handler,
        format="{message}",
        level=0,
        filter=lambda record: record["level"].no >= caplog.handler.level,
        enqueue=False,  # Set to 'True' if your test is spawning child processes.
    )
    yield caplog
    logger.remove(handler_id)


@pytest.fixture
def infrasys_test_system():
    return pjm_2area()
