from setuptools import setup

setup(
    name="mock-r2x-external-plugin",
    version="0.1",
    packages=["r2x_mock_plugin"],
    entry_points={
        "r2x_plugin": ["mock_plugin = r2x_mock_plugin.plugin:create_plugin_components"],
    },
    package_data={"r2x_mock_plugin": ["defaults/*.json"]},
)
