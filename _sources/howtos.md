(howto)=
# How-To's

### ... enable additional verbosity
```console
r2x -vv run --input-model <input_model> --output-model <output_model>
```

### ... convert ReEDS outputs into PLEXOS inputs using a configuration file.
```console
r2x -vv run --config user_dict.yaml
```

### ... convert ReEDS outputs into PLEXOS inputs using the CLI.
```console
r2x -vv run --input-model reeds-US --output-model plexos --solve-year=2035 --weather-year=2012
```

### ... upgrade an old version of ReEDS using the CLI
```console
r2x -vv run <translation_args> --upgrade
```

### ... upgrade an old version of ReEDS using a configuration file
```yaml
input-model: reeds-US
output-model: plexos
upgrade: true
```

### ... override a nested dictionary value using a configuration file
```yaml
input-model: reeds-US
output-model: plexos

tech_to_fuel_pm: # If the key exists, it overrides. If it does not, it merges.
   gas:
      fuel: "GAS"
      type: "BA"
   coal:
      fuel: None
      type: "BA"
```

### ... replace an entire dictionary using a configuration file
```yaml
input-model: reeds-US
output-model: plexos

static_horizons:  # Key of the defaults that you want to replace
   _replace: true # Fully replace the key instead of merge
   Lone Mountain:
      fuel: "GAS"
```

### ... override the fname of a file using a configuration file
```yaml
input-model: reeds-US
output-model: plexos

fmap:
   bio_fuel_price:
      fname: "repbioprice_2030.csv"  # New file name
````


### ... change reports for Plexos using a configuration file
Create a new file that has a JSON with the reports required
```{margin}
To construct this JSON, you need to open the reports panel on Plexos and select
the right categories that match plexos.
```
```json
[
    {
        "child_class": "Battery",
        "collection": "Batteries",
        "object_name": "base_report",
        "parent_class": "System",
        "phase_id": 4,
        "property": "Generation",
        "report_period": true,
        "report_samples": false,
        "report_statistics": false,
        "report_summary": true
    }
]
```

On your configuration file, point to the location of the file
```yaml
input-model: reeds-US
output-model: plexos
plexos_reports: /path/to/file/with/new/reports/file.json
````
