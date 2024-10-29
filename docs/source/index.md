# Welcome to R2X's documentation!

R2X is a Framework for translating inputs/outputs of power system model to “X” model(s).


## Model coverage

![ARTEX](_static/model_coverage.png)



## Roadmap

If you're curious about what we're working on, check out the roadmap:

- [Active issues](https://github.com/NREL/R2X/issues?q=is%3Aopen+is%3Aissue+label%3A%22Working+on+it+%F0%9F%92%AA%22+sort%3Aupdated-asc): Issues that we are actively working on.
- [Prioritized backlog](https://github.com/NREL/R2X/issues?q=is%3Aopen+is%3Aissue+label%3ABacklog): Issues we'll be working on next.
- [Nice-to-have](https://github.com/NREL/R2X/labels/Optional): Nice to have features or Issues to fix. Anyone can start working on (please let us know before you do).
- [Ideas](https://github.com/NREL/R2X/issues?q=is%3Aopen+is%3Aissue+label%3AIdea): Future work or ideas for R2X.



## List of parsers

```{eval-rst}
.. autosummary::
   :nosignatures:

   ~r2x.parser.reeds.ReEDSParser
   ~r2x.parser.plexos.PlexosParser
```

## List of exporters

```{eval-rst}
.. autosummary::
   :nosignatures:

   ~r2x.exporter.plexos.PlexosExporter
   ~r2x.exporter.sienna.SiennaExporter
```

## Model compatibility

```{warning}
This table is not up to date.
```

| R2X Version  | Supported Input Model Versions                            | Supported Output Model Versions          |
|--------------|-----------------------------------------                  |----------------------------------------- |
| 0.3          | Sienna (PowerSystems 3.0)                                 | Nodal Sienna                             |
|              | Sienna (PowerSystems 4.0)                                 | Plexos s 9.000R6, 9.2000R5               |
|              | ReEDS                                                     | Plexos 9.000R6, 9.2000R5                 |
|              | Plexos                                                    | Nodal Plexos                             |
| 0.4          | ReEDS (2024.2.0)                                          | Plexos 9.000R6, 9.200R5                  |
|              | ReEDS (2024.2.0)                                          | Sienna (PowerSystems 4.0)                |
|              | Sienna (PowerSystems 4.0)                                 | Plexos 9.000R6, 9.200R5                  |


```{toctree}
:caption: Getting Started
:hidden: true

install.md
usage.md
CHANGELOG.md
```

```{toctree}
:caption: Developer Guide
:hidden: true

dev/develop.md
dev/git.md
```

```{toctree}
:caption: Modeling PCM
:hidden: true

model/logic.md
model/diagram.md
model/models.md
model/terminology.md
api/enums.md
```


```{toctree}
:caption: Zonal to Nodal
:hidden: true

nodal.md
```

```{toctree}
:caption: Plexos Utils
:hidden: true

dev/plexosdb.md
```

```{toctree}
:caption: API Documentation
:hidden: true

api/config.md
api/system.md
api/plexos.md
api/models.md
api/parsers.md
api/exporters.md
```
