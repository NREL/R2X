import csv
import tempfile
import pathlib
import shutil
import pandas as pd
from r2x.upgrader.functions import apply_header, melt, move_file, rename, set_index
from r2x.upgrader import upgrade_handler


def test_rename(tmp_path):
    test_file = tmp_path.joinpath("test.csv")
    test_file.touch()

    new_file = rename(test_file, "text.csv")
    assert isinstance(new_file, pathlib.Path)
    assert new_file.is_file()
    assert new_file.name == "text.csv"
    assert new_file.resolve().parent == test_file.resolve().parent


def test_move_file(tmp_path):
    test_file = tmp_path.joinpath("test.csv")
    test_file.touch()
    with tempfile.TemporaryDirectory() as tmpdirname:
        output_dir = pathlib.Path(tmpdirname)
        new_file = move_file(test_file, output_dir / "test.csv")
        assert new_file is not None
        assert new_file.is_file()
        assert new_file.name == "test.csv"
        assert new_file.resolve().parent == output_dir.resolve()


def test_melt(caplog):
    """Test the melt function."""
    temp_file = pathlib.Path(tempfile.gettempdir()) / "test_data.csv"
    pd.DataFrame({"i": [1, 2], "r": [3, 4], "Q1": [10, 20], "Q2": [30, 40]}).to_csv(temp_file, index=False)

    melted_data = melt(temp_file)
    assert melted_data.equals(
        pd.DataFrame(
            {
                "i": [1, 2, 1, 2],
                "r": [3, 4, 3, 4],
                "quarter": ["Q1", "Q1", "Q2", "Q2"],
                "value": [10, 20, 30, 40],
            }
        )
    ), "Default melt operation failed"

    temp_file.unlink()

    # Test skip with already melted file
    temp_file = pathlib.Path(tempfile.gettempdir()) / "test_data.csv"
    pd.DataFrame(
        {
            "i": [1, 2, 1, 2],
            "r": [3, 4, 3, 4],
            "quarter": ["Q1", "Q1", "Q2", "Q2"],
            "value": [10, 20, 30, 40],
        }
    ).to_csv(temp_file, index=False)
    melted_data = melt(temp_file)
    assert "has been already melted" in caplog.text


def test_apply_header():
    temp_file = pathlib.Path(tempfile.gettempdir()) / "test_data.csv"
    with open(temp_file, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["A", "B"])  # Header row
        writer.writerow([1, 3])  # Data row 1
        writer.writerow([2, 4])  # Data row 2

    updated_df = apply_header(temp_file, "x,y")
    assert updated_df is not None
    assert updated_df.equals(pd.DataFrame({"x": [1, 2], "y": [3, 4]})), "Header application failed"
    temp_file.unlink()

    temp_file = pathlib.Path(tempfile.gettempdir()) / "test_data.csv"
    with open(temp_file, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["A", "B"])  # Header row
        writer.writerow([1, 3])  # Data row 1
        writer.writerow([2, 4])  # Data row 2
    result = apply_header(temp_file, "a,b")
    assert result is None, "Header should not change but returned a DataFrame"

    updated_df_2 = apply_header(temp_file, "x,y")
    assert updated_df_2 is not None
    assert updated_df_2.equals(pd.DataFrame({"x": [1, 2], "y": [3, 4]})), "Second header application failed"

    temp_file.unlink()


def test_set_index():
    temp_file = pathlib.Path(tempfile.gettempdir()) / "test_data.csv"
    with open(temp_file, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["index", "A", "B"])  # Header row
        writer.writerow([0, 1, 3])  # Data row 1
        writer.writerow([1, 2, 4])  # Data row 2

    updated_df = set_index(temp_file, "test_index")
    assert updated_df is None
    temp_file.unlink()

    temp_file = pathlib.Path(tempfile.gettempdir()) / "test_data.csv"
    with open(temp_file, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["", "B"])  # Header row
        writer.writerow([1, 3])  # Data row 1
        writer.writerow([2, 4])  # Data row 2
    result = set_index(temp_file, "index")
    assert result is not None
    assert result.index.name == "index"
    temp_file.unlink()


def test_upgrade_handler(tmp_path, reeds_data_folder):
    # Move data to new folder to not create backup on the main repo
    reeds_tmp_path = tmp_path / "reeds_folder"
    shutil.copytree(reeds_data_folder, tmp_path / "reeds_folder")

    upgrade_handler(reeds_tmp_path)
    pass
