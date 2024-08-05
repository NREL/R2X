<img src="https://www.nrel.gov/client/img/icon-manufacturing.svg" alt="SWC" align="left" width="192px" height="192px"/>
<img align="left" width="0" height="192px" hspace="10"/>

### R2X
> Model translation parsing tool (ReEDS to X)

[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)[![Python](https://img.shields.io/badge/python-3.10%20|%203.11-blue.svg)](https://img.shields.io/badge/python-3.10%20|%203.11-blue.svg)

<br/>
<br/>


## Table of contents
* [Quick Start](#quick-start)
* [Developer Guide](https://pages.github.nrel.gov/PCM/R2X/dev/develop.html)
* [Roadmap](#roadmap)
* [Compatibility](#compatibility)


## Quick Start

### Prerequisites

```console
mamba env create -f environment.yml && conda activate r2x
```
To install all Python dependencies

```console
python -m pip install -e  ".[all]"
```

### Running the CLI

```console
r2x -i $RUN_FOLDER --year=2035 -o $OUTPUT_FOLDER
```

## Developer environment setup


Before starting to work on adding/fixing on R2X, make sure that you setup your
environment to include all the developer dependencies and our opinionated
pre-commit hooks.

```console
python -m pip install -e ".[dev]"
```

### Install pre-commit hooks
```console
pre-commit install
```

## Roadmap

If you're curious about what we're working on, check out the roadmap:

- [Active issues](https://github.nrel.gov/PCM/R2X/issues?q=is%3Aopen+is%3Aissue+label%3A%22Working+on+it+%F0%9F%92%AA%22+sort%3Aupdated-asc): Issues that we are actively working on.
- [Prioritized backlog](https://github.nrel.gov/PCM/R2X/issues?q=is%3Aopen+is%3Aissue+label%3ABacklog): Issues we'll be working on next.
- [Nice-to-have](https://github.nrel.gov/PCM/R2X/labels/Optional): Nice to have features or Issues to fix. Anyone can start working on (please let us know before you do).
- [Ideas](https://github.nrel.gov/PCM/R2X/issues?q=is%3Aopen+is%3Aissue+label%3AIdea): Future work or ideas for R2X.


## Compatibility

| R2X Version  | Supported Input Model Versions          | Supported Output Model Versions         |
|--------------|-----------------------------------------|-----------------------------------------|
| 0.1          | ReEDS (v1, v2, v3, v4)                  | Plexos 9.000R6                           |
| 0.2          | Sienna (PowerSystem 1.0)                | Nodal Sienna              |
|              | ReEDS                                   | Plexos 9.000R6, 9.2000R5             |
|              | Plexos                                  | Nodal Plexos              |
| 0.3          | Sienna (PowerSystem 1.0)                | Nodal Sienna              |
|              | Sienna (PowerSystem 1.0)<sup><b>*</b></sup>              | Plexos s 9.000R6, 9.2000R5             |
|              | ReEDS                                   | Plexos 9.000R6, 9.2000R5             |
|              | Plexos                                  | Nodal Plexos              |


### Notes:
- **Sienna to Plexos:** Experimental phase
