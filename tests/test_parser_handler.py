from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from r2x.parser.handler import file_handler


def test_file_handler():
    file_content = "name,value\nTemp,25.5\nLoad,1200\nPressure,101.3"
    with NamedTemporaryFile(mode="w+", suffix=".asd", delete=False) as temp_file:
        temp_file.write(file_content)
        temp_file.seek(0)

        with pytest.raises(NotImplementedError):
            _ = file_handler(Path(temp_file.name))
