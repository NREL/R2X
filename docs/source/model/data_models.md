(Generator-Models)=
# PCM Modeling Components

We implemented the data models used follow the
[NREL-Sienna/PowerSystem.jl](https://github.com/NREL-Sienna/PowerSystems.jl)
specification tailored for CEM to PCM convertions.
Here is the list of available representations:

## Generators
```{eval-rst}
.. autosummary::
    ~r2x.models.Generator
    ~r2x.models.RenewableDispatch
    ~r2x.models.RenewableNonDispatch
    ~r2x.models.ThermalStandard
    ~r2x.models.HydroDispatch
    ~r2x.models.HydroEnergyReservoir

```

## Topology
```{eval-rst}
.. autosummary::
    ~r2x.models.Area
    ~r2x.models.LoadZone
    ~r2x.models.ACBus
    ~r2x.models.DCBus
    ~r2x.models.ACBranch
    ~r2x.models.DCBranch
    ~r2x.models.MonitoredLine
    ~r2x.models.TModelHVDCLine
    ~r2x.models.Transformer2W
    ~r2x.models.AreaInterchange
```

## Load components
```{eval-rst}
.. autosummary::
    ~r2x.models.PowerLoad
    ~r2x.models.InterruptiblePowerLoad
```

## Storage models
```{eval-rst}
.. autosummary::
    ~r2x.models.Storage
    ~r2x.models.GenericBattery
    ~r2x.models.HydroPumpedStorage
```

## Hybrid representation
```{eval-rst}
.. autosummary::
    ~r2x.models.HybridSystem
```

## Services
```{eval-rst}
.. autosummary::
    ~r2x.models.Emission
    ~r2x.models.Reserve
    ~r2x.models.TransmissionInterface
```
