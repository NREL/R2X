(howto)=
# How-To's

### ... upgrade an old version of ReEDS
```console
r2x -vv run <translation_args> --upgrade
```

### ... enable additional verbosity
```console
r2x -vv run --input-model <input_model> --output-model <output_model>
```

### ... convert ReEDS outputs into PLEXOS inputs.

Using config file
```console
r2x -vv run --config user_dict.yaml
```

Using the CLI
```console
r2x -vv run --input-model reeds-US --output-model plexos --solve-year=2035 --weather-year=2012
```
