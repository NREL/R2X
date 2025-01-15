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
aditiional settings. For example, for `[ReEDS configuration


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
