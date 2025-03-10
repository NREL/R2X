from setuptools import setup

setup(
    name="mock-r2x-external-plugin",
    version="0.1",
    packages=["module"],
    entry_points={
        "r2x_plugin": ["mock_plugin = module.plugin"],
        "r2x_parser": ["mock_parser = module.plugin:TestExternalParser"],

    },

)
