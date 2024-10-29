# Using R2X

This documentation will assist you to run R2X. We divided in two section, most
people will land on the [cli](#cli), but some users might find useful to use the
proper python API.

(cli)=
## Using the CLI (Recommended)

The CLI will cover most of the use case for R2X. If you followed the
[installation guidelines](#installation), you should be able to run R2X from
any place on your computer as long as the Python environment is activated.

To perform a single translation from ReEDS -> Plexos, you can run:

```{margin}
Replace `$RUN_FOLDER` and `$SOLVE_YEAR` with your custom settings.
```
```bash
python -m r2x -i $RUN_FOLDER \
   --year=$SOLVE_YEAR \
   --input-model=reeds-US \
   --output-model=plexos
```

where `$RUN_FOLDER` needs to be replaced with the absolute path or relative path to the ReEDS run and `$SOLVE_YEAR` with the year of interest.. This will create a new folder inside of it called `r2x_export` that will contain all the translation data.

```{note}
For a full list of the CLI arguments run: `python -m r2x --help`
```


#### Feature flags

R2X enables the use of feature flags (features that could or could not make it
into main). To enable this throught the CLI by passing `--flags feature=true`.
The convention we use is simply pass the feature name (with the exact name is
defined in the code).

An example of a curent feature flag:

```{code-block} bash
python -m r2x -i $RUN_FOLDER --year=$SOLVE_YEAR --flags tx-out=true
```


### Using a cases file

```{note}
Cases file is a legacy way of using R2X that was carry over from ReEDS.
Althought, we recognize its functionality, we recommend using the CLI +
configuration file since it is more easily editable by third-party software.
```

Create cases_*.csv file with your scenario specifications. (Copy [cases_default.csv](https://github.com/NREL/R2X/blob/main/cases_test.csv) as template).

Each scenario you are running should have its own column, starting with Column
D Default values in column B are used for all scenarios unless overwritten in
that scenario’s column.

Adjusting the subscenario argument in cases_*.csv can assist to ensure you do not overwrite previous ReEDS_to_PLEXOS conversions by other users in the ReEDS run_folder specified.

```{note}
Make sure if you’re directing run_folder to “D:/” or similar, use “/d/” instead. If you have mapped a network drive with a specific drive letter (W:/, X:/, Y:/, etc.), utilize this mapping convention (and not the underlying
network location (e.g. \\nrelnas01ReEDSFY20-SFSNov25).
```

The syntax to run R2X with a cases file configuration is similar, you just need to pass `--cases` flag on the CLI as such:

```{code-block} console
python -m r2x --cases=$CASES_PATH
```

For additional detail on the implementation of each of the Generator models, see [Models section](#generator-models).


### Nodal
Whether or not a translation is treated as zonal-to-nodal is determined by the input and output models specified in the CLI or cases file. For instance, to perform a zonal-to-nodal translation that applies the results of a ReEDS run to a Sienna model, you can run:

```bash
python -m r2x -im sienna -om sienna -i $RUN_FOLDER --year=$SOLVE_YEAR
```
