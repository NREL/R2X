import pytest
from r2x.runner import init, run


def test_runner(tmp_path, reeds_data_folder):
    cli_input = {
        "name": "Test",
        "weather_year": 2015,
        "solve_year": [2055],
        "input_model": "reeds-US",
        "output_model": "sienna",
        "output_folder": str(tmp_path),
        "feature_flags": {"cool-feature": True},
        "run_folder": reeds_data_folder,
    }

    _ = run(cli_input, {})


def test_runner_exceptions(tmp_path, reeds_data_folder):
    cli_input = {
        "name": "Test",
        "weather_year": 2015,
        "solve_year": [2055, 2050],
        "input_model": "reeds-US",
        "output_model": "sienna",
        "output_folder": str(tmp_path),
        "feature_flags": {"cool-feature": True},
        "run_folder": reeds_data_folder,
    }

    with pytest.raises(NotImplementedError):
        _ = run(cli_input, {})


def test_runner_serialization(tmp_path, reeds_data_folder):
    cli_input = {
        "name": "Test",
        "weather_year": 2015,
        "solve_year": [2055],
        "input_model": "reeds-US",
        "output_model": "sienna",
        "output_folder": str(tmp_path),
        "feature_flags": {"cool-feature": True},
        "run_folder": reeds_data_folder,
        "save": True,
    }

    _ = run(cli_input, {})
    assert (tmp_path / f"{cli_input['name']}.json").exists()


def test_init(tmp_path):
    cli_input = {"path": str(tmp_path)}
    _ = init(cli_input)
    assert (tmp_path / "user_dict.yaml").exists()
