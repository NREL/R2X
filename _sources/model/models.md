# Translation Configuration

## Scenario Models

The default workflow of R2X creates an {class}`~r2x.config_scenario.Scenario` instance that
keeps track of the default information required to do a translation.

```{note}
This class gets created when calling `r2x run` from a terminal
```

The recommended way of creating an instance of this class is to call
{meth}`~r2x.config_scenario.Scenario.from_kwargs` like this example:

```python
kwargs = {
    "name": "Test",
    "weather_year": 2015,
    "solve_year": 2055,
    "output_folder": tmp_folder,
    "input_model": "reeds-US",
    "output_model": "sienna",
    "feature_flags": {"cool-feature": True},
}
scenario = Scenario.from_kwargs(**kwargs)
```

For additional implementation details of the configuration models see the
[API](#scenario-models)

## Configuration Models
R2X depends on [configuration classes](#configuration) that specifies
the model that we are reading or exporting to. Each model supported by R2X has
its own class and fields that are required. An example, for the {term}`ReEDS`
model you can directly create an instance of the class and specify the fields
for the translation you will perform.

```python
from r2x.config_models import ReEDSConfig
reeds_config = ReEDSConfig(solve_year=2050, weather_year=2012)
```

In addition to the configuration of each model, fields defined in one model
needs to be mapped to another field for another model. A clear example of this
is when we are translating from {term}`ReEDS` to {term}`PLEXOS`. Each class
should implement {meth}`~r2x.config_models.BaseModelConfig.get_field_mapping`
that specifies how we convert from model configuration to another. Here is a
simplified example from the {class}`~r2x.config_models.ReEDSConfig`,

```python
class ReEDSConfig(BaseModelConfig):

    solve_year: list[int] | int | None = None
    weather_year: int | None = None
    ...

    def get_field_mapping(cls) -> dict[type[BaseModel], dict[str, str]]:
        """Return a dict of {target_class: {target_field: source_field}}."""
        return {
            PlexosConfig: {
                "model_year": "solve_year",
                "horizon_year": "weather_year",
            },
            SiennaConfig: {"model_year": "solve_year"},
        }
```

On the ReEDS class, we mapped the field `solve_year` to `model_year` for both
{class}`~r2x.config_models.PlexosConfig` and
{class}`~r2x.config_models.SiennaConfig`. The syntax to provide the field
mappings is to add new keys with the subclass of
{class}`~r2x.config_models.BaseModelConfig` and dictionary for each key tha
requires mapping. The mapping is performed automatically when calling an
exporter (for more details see
{meth}`~r2x.config_models.BaseModelConfig.to_class`).

For additional implementation details of the configuration models see the
[API](#configuration-models)
