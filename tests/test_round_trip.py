from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.runner import run_exporter, run_parser


def test_round_trip(tmp_folder, reeds_data_folder):
    reeds_infrasys_config = Scenario.from_kwargs(
        name="from_reeds",
        input_model="reeds-US",
        output_model="infrasys",
        run_folder=reeds_data_folder,
        output_folder=tmp_folder,
        solve_year=2050,
        model_year=2050,
        weather_year=2012,
    )
    infrasys_plexos_config = Scenario.from_kwargs(
        name="from_reeds",
        input_model="infrasys",
        output_model="plexos",
        run_folder=tmp_folder,
        output_folder=tmp_folder,
        solve_year=2050,
        model_year=2050,
        weather_year=2012,
    )

    user_dict = {"plexos_category_map": reeds_infrasys_config.input_config.defaults["tech_to_fuel_pm"]}
    plexos_infrasys_config = Scenario.from_kwargs(
        name="from_plexos",
        input_model="plexos",
        output_model="infrasys",
        run_folder=tmp_folder,
        output_folder=tmp_folder,
        solve_year=2050,
        model_year=2050,
        weather_year=2012,
        model_name="model_2012",
        user_dict=user_dict,
    )

    # Export ReEDS to infrasys
    orignal_system_fpath = tmp_folder / reeds_infrasys_config.name / ".json"
    original_system, parser = run_parser(reeds_infrasys_config)
    original_system.to_json(orignal_system_fpath)

    # Export infrasys to plexos

    deserialized_original = System.from_json(orignal_system_fpath)
    _ = run_exporter(infrasys_plexos_config, deserialized_original)

    # Export plexos to infrasys
    deserialized_plexos, parser = run_parser(plexos_infrasys_config)

    assert (
        original_system._components.get_num_components()
        == deserialized_plexos._components.get_num_components()
    )
