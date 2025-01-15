(getting-started)=
# Getting Started

Once you have [R2X installed](#installation), you can check the
installation by running the `r2x` command in a terminal:

```{code-block} console
$ r2x --help

usage: r2x [-h] [--verbose] [--version] {init,run} ...
Model translation framework

positional arguments:
  {init,run}     Subcommands
    init         Create an empty configuration file.
    run          Run an R2X translation

options:
  -h, --help     show this help message and exit
  --verbose, -v  Run with additional verbosity
  --version, -V  show program's version number and exit
```


(run)=
## `r2x run` overview

The `r2x run` command is the main entry point for performing translations between
different models. It supports two primary workflows for specifying input
arguments:

1. Using a configuration file (recommended),
2. Using the positional arguments,

### Using a configuration file

This is the recommended approach because it provides a human-readable format
that can be saved and reused for future translations. The configuration file
preserves all necessary information about the translation process.

```{margin}
To create an example configuration file you can run [`r2x init`](#init)
```
```yaml
input_model: reeds-US
output_model: sienna
solve_year: 2050
weather_year: 2012
```

Depending on the parser/exporter combo used, the configuration will require
aditiional settings (see the [configuration model
classes](#configuration-models) for each model specific field list)

The configuration file also enables to do multiple concurrent translations. To
use this functionality, you need to create a new key called `scenarios` (see
example below)

```{margin}
Each scenario **must** have a unique name. Otherwise, R2X will overwrite the
resulting translation.
```
```
input_model: reeds-US
output_model: sienna
weather_year: 2012
scenarios:
 - { name Run1, solve_year: 2050}
 - { name Run2, solve_year: 2050}
```

All the keys that are not specified on the scenario are considered global and
will be applied for all the scenarios.


```{note}
Scenarios only work with a configuration file.
```

#### Command example

```{margin}
You need to point to the relative path to the `config.yaml` file.
```
```console
r2x run --config config.yaml
```

### Using positional arguments

If you prefer not to use a configuration file, you can directly provide input
arguments as positional parameters.

**Syntax**

```console
r2x run run --input-model <input_model> --output-model <output_model>
```

**Example command**
```{margin}
Replace `$RUN_FOLDER` and `$SOLVE_YEAR` with your custom settings.
```
```console
r2x -i $RUN_FOLDER \
   --solve-year=$SOLVE_YEAR \
   --input-model=reeds-US \
   --output-model=plexos
```

### Experimental features

R2X enables the use of feature flags (features that could or could not make it
into main). To enable this throught the CLI by passing `--flags feature=true`.
The convention we use is simply pass the feature name (with the exact name is
defined in the code).

An example of a curent feature flag:

```{code-block} bash
r2x -i $RUN_FOLDER --year=$SOLVE_YEAR --flags tx-out=true
```

For additional detail on the implementation of each of the PCM models, see [Models section](#generator-models).

(init)=
## `r2x init` overview

The `r2x init` command is used to create a copy of the `user_dict.yaml` in the
current location where you call the command. This will contain default
information that can be changed to the translation you will be performing.

A typical workflow might look like the following, let's say that we want to
translate {term}`ReEDS` results into {term}`PLEXOS` and we want to translate 2
`solve_years` 2035 and 2050. The first thing create a
configuration file

```console
r2x init
```

then with any text editor of your choice modify the `user_dict.yaml` file to
specify the run_folder, output_folder (optional) and the scenarios to run.

```yaml
input_model: reeds-US
output_model: sienna
run_folder: /path/to/input_model/
output_folder: /path/to/save/results
scenarios:
  - {name: Run1, solve_year: 2035}
  - {name: Run2, solve_year: 2040}
```

Once everything is setup, you run the `r2x run` command

```console
r2x run --config user_dict.yaml
```
