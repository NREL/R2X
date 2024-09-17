# Data Model


## Topology components

```{eval-rst}
.. autopydantic_model:: r2x.model.Area
   :inherited-members: Component

.. autopydantic_model:: r2x.model.LoadZone

.. autopydantic_model:: r2x.model.ACBus
   :inherited-members: Bus

.. autopydantic_model:: r2x.model.DCBus
   :inherited-members: Bus

.. autopydantic_model:: r2x.model.DCBranch
   :inherited-members: Branch

.. autopydantic_model:: r2x.model.ACBranch
   :inherited-members: Branch

.. autopydantic_model:: r2x.model.MonitoredLine
   :inherited-members: ACBranch

.. autopydantic_model:: r2x.model.TModelHVDCLine
   :inherited-members: DCBranch

.. autopydantic_model:: r2x.model.Transformer2W
   :inherited-members: ACBranch

.. autopydantic_model:: r2x.model.TransmissionInterface


```

## Load components

```{eval-rst}
.. autopydantic_model:: r2x.model.PowerLoad

.. autopydantic_model:: r2x.model.FixedLoad

.. autopydantic_model:: r2x.model.InterruptiblePowerLoad
```


## Generator models

```{eval-rst}
.. autopydantic_model:: r2x.model.Generator

.. autopydantic_model:: r2x.model.RenewableGen
   :inherited-members: Generator

.. autopydantic_model:: r2x.model.RenewableDispatch
   :inherited-members: RenewableGen

.. autopydantic_model:: r2x.model.RenewableFix
   :inherited-members: RenewableGen

.. autopydantic_model:: r2x.model.ThermalGen
   :inherited-members: Generator

.. autopydantic_model:: r2x.model.ThermalStandard
   :inherited-members: ThermalGen

.. autopydantic_model:: r2x.model.ThermalMultiStart
   :inherited-members: ThermalGen

.. autopydantic_model:: r2x.model.HydroGen
   :inherited-members: Generator

.. autopydantic_model:: r2x.model.HydroFix
   :inherited-members: HydroGen

.. autopydantic_model:: r2x.model.HydroDispatch
   :inherited-members: HydroGen

.. autopydantic_model:: r2x.model.HydroEnergyReservoir
   :inherited-members: HydroGen
```


## Storage models

```{eval-rst}
.. autopydantic_model:: r2x.model.Storage
   :inherited-members: Generator

.. autopydantic_model:: r2x.model.GenericBattery
   :inherited-members: Storage

.. autopydantic_model:: r2x.model.HydroPumpedStorage
   :inherited-members: HydroGen

```

## Hybrid representation

```{eval-rst}
.. autopydantic_model:: r2x.model.HybridSystem
```

## Services

```{eval-rst}
.. autopydantic_model:: r2x.model.Service

.. autopydantic_model:: r2x.model.Emission

.. autopydantic_model:: r2x.model.Reserve
```
