(Generator-Models)=
# Data Model


We implemented the data model used follow the
[NREL-Sienna/PowerSystem.jl](https://github.com/NREL-Sienna/PowerSystems.jl)
specification tailored for CEM to PCM convertions.
Here is the list of available representations:

## Generators

```{eval-rst}
.. autosummary::
    ~r2x.model.Generator
    ~r2x.model.RenewableGen
    ~r2x.model.RenewableDispatch
    ~r2x.model.RenewableFix
    ~r2x.model.ThermalGen
    ~r2x.model.ThermalStandard
    ~r2x.model.ThermalMultiStart
    ~r2x.model.HydroGen
    ~r2x.model.HydroFix
    ~r2x.model.HydroDispatch
    ~r2x.model.HydroEnergyReservoir

```


## Topology

```{eval-rst}
.. autosummary::
    ~r2x.model.Area
    ~r2x.model.LoadZone
    ~r2x.model.ACBus
    ~r2x.model.DCBus
    ~r2x.model.ACBranch
    ~r2x.model.DCBranch
    ~r2x.model.MonitoredLine
    ~r2x.model.TModelHVDCLine
    ~r2x.model.Transformer2W
    ~r2x.model.TransmissionInterface
```

## Load components

```{eval-rst}
.. autosummary::
    ~r2x.model.PowerLoad
    ~r2x.model.FixedLoad
    ~r2x.model.InterruptiblePowerLoad
```

## Storage models

```{eval-rst}
.. autosummary::
    ~r2x.model.Storage
    ~r2x.model.GenericBattery
    ~r2x.model.HydroPumpedStorage
```

## Hybrid representation
```{eval-rst}
.. autosummary::
    ~r2x.model.HybridSystem
```

## Services

```{eval-rst}
.. autosummary::
    ~r2x.model.Service
    ~r2x.model.Emission
    ~r2x.model.Reserve
```
