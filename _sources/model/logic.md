# Baseline System

On R2X, we refer as {term}`baseline system` as the processed representation of
the grid that adopts the Sienna Data Model. The system includes all the
components of the grid including the topology (e.g., buses, branches), assets
(transformers, generators, storage) and additional components that are used for
modeling purposes (e.g., emissions, time series). The purpose of the system is
to create a standarized representation of the grid that can be used to convert
from and to different models.

For creating the baseline system, we use
[infrasys](https://nrel.github.io/infrasys/), a python package that implements a
data store for components and time series that allows for seamless data model
migration.

Once the system is created, we pass the system to the different exporters that
will create the input files for a given model.
