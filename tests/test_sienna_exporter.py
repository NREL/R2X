import pytest

from r2x.config_scenario import Scenario
from r2x.exporter.sienna import SiennaExporter, apply_operation_table_data, get_psy_fields


@pytest.fixture
def scenario_instance(data_folder, tmp_folder):
    return Scenario.from_kwargs(
        name="Test Scenario",
        run_folder=data_folder,
        output_folder=tmp_folder,
        input_model="infrasys",
        output_model="sienna",
        model_year=2010,
    )


@pytest.fixture
def sienna_exporter(scenario_instance, infrasys_test_system, tmp_folder):
    return SiennaExporter(config=scenario_instance, system=infrasys_test_system, output_folder=tmp_folder)


@pytest.mark.sienna
def test_sienna_exporter_instance(sienna_exporter):
    assert isinstance(sienna_exporter, SiennaExporter)


@pytest.mark.sienna
def test_sienna_exporter_run(sienna_exporter, tmp_folder):
    exporter = sienna_exporter.run()

    output_files = [
        "gen.csv",
        "bus.csv",
        "timeseries_pointers.json",
        # "storage.csv",  # Storage is also optional
        "reserves.csv",  # Reserve could be optional
        "dc_branch.csv",
        "branch.csv",
    ]

    for file in output_files:
        assert (tmp_folder / file).exists(), f"File {file} was not created properly."

    # Check that time series was created correctly
    ts_directory = tmp_folder / exporter.ts_directory
    assert any(ts_directory.iterdir())


def test_sienna_exporter_empty_storage(caplog, sienna_exporter):
    sienna_exporter.process_storage_data()
    assert "No storage devices found" in caplog.text


@pytest.fixture
def sample_component():
    return {
        "operation_cost": {
            "variable": {
                "vom_cost": {"function_data": {"proportional_term": 10}},
                "fuel_cost": 0.05,
                "value_curve": {
                    "function_data": {
                        "constant_term": 100,
                        "proportional_term": 20,
                        "quadratic_term": 0.5,
                        "points": [(0, 0), (50, 1000), (100, 2500)],
                    }
                },
            },
            "variable_type": "CostCurve",
        }
    }


def test_get_psy_fields():
    fields = get_psy_fields()
    assert isinstance(fields, dict)


def test_apply_operation_table_data_basic(sample_component):
    updated_component = apply_operation_table_data(sample_component)

    assert "variable_cost" in updated_component
    assert "fuel_price" in updated_component
    assert updated_component["variable_cost"] == 10
    assert updated_component["fuel_price"] == 50  # 0.05 * 1000


def test_apply_operation_table_data_heat_rate(sample_component):
    updated_component = apply_operation_table_data(sample_component)

    assert "heat_rate_a0" in updated_component
    assert "heat_rate_a1" in updated_component
    assert "heat_rate_a2" in updated_component
    assert updated_component["heat_rate_a0"] == 100
    assert updated_component["heat_rate_a1"] == 20
    assert updated_component["heat_rate_a2"] == 0.5


def test_apply_operation_table_data_cost_curve(sample_component):
    updated_component = apply_operation_table_data(sample_component)

    assert "output_point_0" in updated_component
    assert "cost_point_0" in updated_component
    assert updated_component["output_point_0"] == 0
    assert updated_component["cost_point_0"] == 0
    assert updated_component["output_point_1"] == 50
    assert updated_component["cost_point_1"] == 1000


def test_apply_operation_table_data_fuel_curve():
    fuel_curve_component = {
        "operation_cost": {
            "variable": {"value_curve": {"function_data": {"points": [(0, 0), (50, 10), (100, 25)]}}},
            "variable_type": "FuelCurve",
        }
    }
    updated_component = apply_operation_table_data(fuel_curve_component)

    assert "output_point_0" in updated_component
    assert "heat_rate_avg_0" in updated_component
    assert updated_component["output_point_0"] == 0
    assert updated_component["heat_rate_avg_0"] == 0
    assert updated_component["output_point_1"] == 50
    assert updated_component["heat_rate_incr_1"] == 10


def test_apply_operation_table_data_no_operation_cost():
    component = {"id": "test_component"}
    updated_component = apply_operation_table_data(component)
    assert updated_component == component


def test_apply_operation_table_data_no_variable():
    component = {"operation_cost": {}}
    updated_component = apply_operation_table_data(component)
    assert updated_component == component


def test_apply_operation_table_data_unsupported_curve():
    component = {
        "operation_cost": {
            "variable": {"value_curve": {"function_data": {"points": [(0, 0), (50, 10), (100, 25)]}}},
            "variable_type": "UnsupportedCurve",
        }
    }
    with pytest.raises(NotImplementedError):
        apply_operation_table_data(component)


def test_apply_operation_table_data_none_fuel_cost():
    component = {"operation_cost": {"variable": {"fuel_cost": None}}}
    with pytest.raises(AssertionError):
        apply_operation_table_data(component)
