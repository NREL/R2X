"""R2X pytest fixtures.

Here goes all the variables that will be shared between all the different testing scripts.
"""

import pytest
from r2x.utils import read_json
from loguru import logger
from _pytest.logging import LogCaptureFixture


DATA_FOLDER = "tests/data"
OUTPUT_FOLDER = "r2x_output"


@pytest.fixture
def data_folder(pytestconfig):
    return pytestconfig.rootpath.joinpath(DATA_FOLDER)


@pytest.fixture
def tmp_folder(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp(OUTPUT_FOLDER)
    yield tmp_path


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
