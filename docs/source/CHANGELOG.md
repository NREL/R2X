# Changelog

All notable changes to this project will be documented in this file. See [conventional commits](https://www.conventionalcommits.org/) for commit guidelines.

---
## [0.5.2](https://github.com/NREL/R2X/compare/v0.5.1..v0.5.2) - 2024-07-16

### Bug Fixes

- **(plexos_export)** Add proper handling of Fixed load to regions (#398) - ([98819e2](https://github.com/NREL/R2X/commit/98819e228d533541dde774c1f7ab24eba878eb93)) - Sanchez Perez, Pedro Andres
- **(plexos_export)** Adding correct property for Battery objects (#399) - ([45e2eea](https://github.com/NREL/R2X/commit/45e2eea4c6aa27af1cbb72ac1f07aa28bdb5a602)) - Sanchez Perez, Pedro Andres
- **(plexos_exporter)** Re-enabled transmission interfaces to the model (#393) - ([a591481](https://github.com/NREL/R2X/commit/a5914819803caa5fdc981b4ef5be79776d6dc64f)) - Sanchez Perez, Pedro Andres
- **(plexos_parser)** Restoring functionality of the Plexos parser using the new data model. (#396) - ([ab3a791](https://github.com/NREL/R2X/commit/ab3a7912782d4f6f84b8296668a9065b08ab6932)) - Sanchez Perez, Pedro Andres
- **(plexos_reserve)** Update reserve representation on plexos (#392) - ([b2bf1ab](https://github.com/NREL/R2X/commit/b2bf1ab3de1306eddc697061c4161e1e23c7f676)) - Sanchez Perez, Pedro Andres
- **(reeds_parser)** Fixing weather year time index (#390) - ([644c21c](https://github.com/NREL/R2X/commit/644c21cbda13e7c70aea31fd568679011f12882d)) - Liu, Vivienne
- **(sienna_exporter)** Fix `timeseries_pointers` serialization problem (#395) - ([73782e5](https://github.com/NREL/R2X/commit/73782e5ee053568be4ee0fdccd5d9f2fbbb4984d)) - Sanchez Perez, Pedro Andres

### Update

- **(plexosdb)** Renaming `plexos_db` to `plexosdb` (#400) - ([8189eb8](https://github.com/NREL/R2X/commit/8189eb88f56df7734220c3e27134a87d4bfbc748)) - Sanchez Perez, Pedro Andres

---
## [0.5.1](https://github.com/NREL/R2X/compare/v0.5.0..v0.5.1) - 2024-06-18

### Bug Fixes

- **(plexos)** Enable opinionated reports for plexos export (#382) - ([a852a45](https://github.com/NREL/R2X/commit/a852a45dee3607d5338f2051d0fbcc67a1c27b19)) - Sanchez Perez, Pedro Andres
- **(plexos)** Fix problem with the unit conversion for some fields. (#385) - ([220eacf](https://github.com/NREL/R2X/commit/220eacf4ad10ec5375bc0f310fab1c47f1049211)) - Sanchez Perez, Pedro Andres
- **(sqlite)** Added batched insert for tags of properties (#384) - ([4d6aa47](https://github.com/NREL/R2X/commit/4d6aa47985d3bc566e45b67cb5ed7295e6e86bb2)) - Sanchez Perez, Pedro Andres
- **(time_series_files)** Restored naming convention for time series time_series_files (#386) - ([729964d](https://github.com/NREL/R2X/commit/729964dd8cb48f599ef116f198f301b155651cbb)) - Sanchez Perez, Pedro Andres

---
## [0.5.0](https://github.com/NREL/R2X/compare/v0.4.1..v0.5.0) - 2024-06-07

### Bug Fixes

- **(bumpversion)** Updated string to reflect src layout. - ([b807412](https://github.com/NREL/R2X/commit/b807412a9e487ae706b4207951570182a59d7fe9)) - pesap
- **(cli)** Cleaned CLI to be more consise and added infrasys as input model. (#350) - ([3241531](https://github.com/NREL/R2X/commit/3241531771d2a34dc56bbd5ef282830586924b86)) - Sanchez Perez, Pedro Andres
- **(reeds_parser)** Removing generators that have zero capacity as a patch. (#378) - ([5060611](https://github.com/NREL/R2X/commit/5060611c6b49f758942c8850d334582baffc3db1)) - Sanchez Perez, Pedro Andres
- **(sienna_exporter)** Cleaning documentation of exporter and closing issues. (#349) - ([7499d1d](https://github.com/NREL/R2X/commit/7499d1df16505094b92eeb6ca6b609c77c21c607)) - Sanchez Perez, Pedro Andres

### Features

- **(pint_units)** Added pint unit support to the reeds parser and plexos exporter (#366) - ([740a852](https://github.com/NREL/R2X/commit/740a8522a745cb395737420b2557f4971b753399)) - Sanchez Perez, Pedro Andres
- **(plexos_parser)** Enhancing the functionality of the Plexos parser (#376) - ([89ec82c](https://github.com/NREL/R2X/commit/89ec82cf6e0515b36e0c12a9089a3c03fefa8691)) - Sanchez Perez, Pedro Andres

### Miscellaneous Chores

- **(enum)** Added PrimeMoversType enum  (#358) - ([a2b30bc](https://github.com/NREL/R2X/commit/a2b30bc3e3155b7d443751cdcbedc3766d4c3c39)) - Fuchs, Rebecca
- **(files)** Renaming files for more consistency (#354) - ([b901a34](https://github.com/NREL/R2X/commit/b901a34523b68a00860a129d15418a21ccafe7e4)) - Sanchez Perez, Pedro Andres

### Update

- **(r2x)** Updating Plexos back-end and src/layout for the project (#373) - ([e3dba03](https://github.com/NREL/R2X/commit/e3dba032c911b3540fe800ebf8e31915ecdeccdc)) - Sanchez Perez, Pedro Andres

---
## [0.4.1](https://github.com/NREL/R2X/compare/v0.4.0..v0.4.1) - 2024-04-17

### Bug Fixes

- **(reeds_mapping)** Updated reeds mapping to match how we use it internally. (#327) - ([cc59bf1](https://github.com/NREL/R2X/commit/cc59bf11403142140257ff528ffa4fa33037aee6)) - Sanchez Perez, Pedro Andres

### Features

- **(enum)** Added new enum to represent different reserve types. - ([eaeb34d](https://github.com/NREL/R2X/commit/eaeb34d95cbe6343cd087d7a2756ca594c7f4993)) - Fuchs, Rebecca

### Update

- **(Sienna_exporter)** Cleaning Sienna exporter to use the new data model. (#329) - ([e8165e2](https://github.com/NREL/R2X/commit/e8165e2ddbb966d1c152f4661b9b67b510204427)) - Sanchez Perez, Pedro Andres

---
## [0.4.0](https://github.com/NREL/R2X/compare/v0.3.1..v0.4.0) - 2024-03-27

### Bug Fixes

- **(cambium)** Tech mapping capability and bugfix of PTC implementation (#306) - ([122c165](https://github.com/NREL/R2X/commit/122c165d6d288d4ea5f7afed3ebb3935c1c127b0)) - Sanchez Perez, Pedro Andres
- **(defaults)** Translator was picking incorrect name for the transmission losses causing it to be skipped on Plexos. (#299) - ([761927e](https://github.com/NREL/R2X/commit/761927ecdea115bc2884ca44d089ee141e814d0d)) - Sanchez Perez, Pedro Andres
- **(heat_rates)** Adding additional technologies to the heat rate fits and fixing some electrolyzer. (#284) - ([701e719](https://github.com/NREL/R2X/commit/701e719ce1157762456eaace2312cb17b6f7c4e7)) - Sanchez Perez, Pedro Andres
- **(nodal)** Overhauled Sienna parsing procedure and improved overall nodal process logic and testing (#259) - ([80c8861](https://github.com/NREL/R2X/commit/80c88614e751549ba4355916b428b7ff05f29ff4)) - Sanchez Perez, Pedro Andres
- **(sienna)** Adding canada imports and hydro profiles to the exporter (#276) - ([2a7d7ab](https://github.com/NREL/R2X/commit/2a7d7ab2089deeb3eda97be83e802adae3d46d70)) - Sanchez Perez, Pedro Andres
- **(translator)** Removing timezone since ReEDS incorporate it now. (#278) - ([7775094](https://github.com/NREL/R2X/commit/77750948701dca6f2eba353b1598a79b5836de97)) - Sanchez Perez, Pedro Andres

### Documentation

- **(sphinx)** Creating new documentation using sphinx (#311) - ([acc5a78](https://github.com/NREL/R2X/commit/acc5a78bd46f8027648df3d25044832402613297)) - Sanchez Perez, Pedro Andres
- Adding the documentation (#294) - ([5f8e8be](https://github.com/NREL/R2X/commit/5f8e8be54aaa7858ac2f73fc4307db35e5cbe948)) - Sanchez Perez, Pedro Andres

### Features

- **(docs)** Automatic build of documentation (#293) - ([288e097](https://github.com/NREL/R2X/commit/288e097a8d775ed64019b4697bd8399a663f0221)) - Sanchez Perez, Pedro Andres
- **(plugin)** PTC implementation (#272) - ([7594ba1](https://github.com/NREL/R2X/commit/7594ba12347facbc70a49740ce968dac3020422a)) - Schwarz, Marty
- **(reeds)** Adding additional flags that activate the electrolyzer plugin. (#282) - ([a9464c2](https://github.com/NREL/R2X/commit/a9464c27286340b653ed0b57a62b4e0cb9613d31)) - Sanchez Perez, Pedro Andres

### Miscellaneous Chores

- **(docs)** Cleaning requirements.txt (#321) - ([da1ec7a](https://github.com/NREL/R2X/commit/da1ec7a2f04b137600b240b01727f1e62616f5b6)) - Sanchez Perez, Pedro Andres

### Bug

- **(nodal)** Added techs to new build names to de-dupe them (#290) - ([5d1978c](https://github.com/NREL/R2X/commit/5d1978ccb7ca7712d5cf3e95e22ff437238c1035)) - Obika, Kodi
- **(nodal)** PSS name convention was not being added into the parser. (#291) - ([baccbb8](https://github.com/NREL/R2X/commit/baccbb8bbaf2736444c985bdec99551b4dd6318b)) - Sanchez Perez, Pedro Andres

### Update

- **(cambium)** Adding some new features for cambium and some cleaning of the CLI (#310) - ([e0d5a39](https://github.com/NREL/R2X/commit/e0d5a393ef6a2a5aeac5982c82085990f93709c6)) - Sanchez Perez, Pedro Andres
- **(data_model)** Changing Translator data model (#303) - ([c0caf87](https://github.com/NREL/R2X/commit/c0caf87a6048957b5a31e446205d773013f3afbf)) - Sanchez Perez, Pedro Andres
- **(docs)** Rewriting some of the documentation (#288) - ([caa4d53](https://github.com/NREL/R2X/commit/caa4d53174721d2e50226eae380231d638647ea3)) - Sanchez Perez, Pedro Andres
- **(pyproject.toml)** Add gamspy to dependency list. (#277) - ([a13bc11](https://github.com/NREL/R2X/commit/a13bc11e44062bdb7655734a441d1afa2af7823a)) - Schwarz, Marty

---
## [0.3.1](https://github.com/NREL/R2X/compare/v0.3.0..v0.3.1) - 2023-11-16

### Bug Fixes

- **(defaults)** Removing distribution losses since the load.h5 already incorporate them. (#261) - ([6b9f0fe](https://github.com/NREL/R2X/commit/6b9f0feb39232a808d30dce434052776773e983e)) - Sanchez Perez, Pedro Andres
- **(infeaisibility)** Changing hydro modeling to avoid infeasibilities. (#257) - ([0c755a1](https://github.com/NREL/R2X/commit/0c755a12d715198a885376a0cbc7f22fcb809cf3)) - Sanchez Perez, Pedro Andres
- **(memberships)** Fixing scenario name missing from custom memberships. (#265) - ([786554c](https://github.com/NREL/R2X/commit/786554c4b3770d9fccfa63109ccf9583f3e63c6c)) - Sanchez Perez, Pedro Andres
- **(plexos)** Adding VoLL to each region to avoid bug in Plexos. (#268) - ([9cf0d87](https://github.com/NREL/R2X/commit/9cf0d87fa06bafae979d0501a68dcf88427dbf57)) - Sanchez Perez, Pedro Andres
- **(plugins)** Fixing LDES plugin aggregation and adding more attributes to storage assets. (#260) - ([4b89005](https://github.com/NREL/R2X/commit/4b8900538ca5690d6963787ca95c7ab42b778047)) - Sanchez Perez, Pedro Andres

### Features

- **(translator)** Adding option for better heat rate representation. (#266) - ([8e95f57](https://github.com/NREL/R2X/commit/8e95f57f2018228f1ff3dd985e07a267edb6652c)) - Sanchez Perez, Pedro Andres

### Miscellaneous Chores

- **(plugins)** Cleaning electrolyzer module. - ([4965e3e](https://github.com/NREL/R2X/commit/4965e3ece434f2efcf58ef9700401f7ea139be9d)) - pesap

<!-- generated by git-cliff -->
