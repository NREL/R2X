import pytest
from r2x.config_scenario import Scenario
from r2x.runner import run_single_scenario


@pytest.mark.parametrize(
    "input_model, output_model, name, run_folder",
    [
        ("reeds-US", "sienna", "R2S", "reeds_data_folder"),
        ("reeds-US", "plexos", "R2P", "reeds_data_folder"),
        ("reeds-US", "infrasys", "R2I", "reeds_data_folder"),
        ("infrasys", "sienna", "pjm_2area", "infrasys_data_folder"),
        ("infrasys", "plexos", "pjm_2area", "infrasys_data_folder"),
    ],
    ids=["R2S", "R2P", "R2I", "I2S", "I2P"],
)
def test_full_translation_workflow(input_model, output_model, name, run_folder, request, tmp_folder):
    run_folder = request.getfixturevalue(run_folder)
    config = Scenario.from_kwargs(
        name=name,
        input_model=input_model,
        output_model=output_model,
        run_folder=run_folder,
        output_folder=tmp_folder,
        solve_year=2050,
        model_year=2050,
        weather_year=2012,
    )

    _ = run_single_scenario(scenario=config)
