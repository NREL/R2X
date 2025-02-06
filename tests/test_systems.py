from r2x.api import System


def test_serialization(infrasys_test_system, tmp_path):
    system = infrasys_test_system
    system.to_json(tmp_path / "test.json")
    deserialized_system = System.from_json(tmp_path / "test.json")

    assert system._uuid == deserialized_system._uuid
    assert system._components.get_num_components() == deserialized_system._components.get_num_components()
