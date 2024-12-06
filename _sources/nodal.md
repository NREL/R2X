# Nodal documentation

```{role} python(code)
:language: python
```

## Overview

The zonal-to-nodal component of R2X is a process by which the results of a
zonal capacity expansion model ({term}`CEM`) can be applied to a nodal production cost
model ({term}`PCM`). This exists as four main steps:
1.	Determine the zone-wise and technology-wise capacity that needs to be added
	to and removed from the PCM to match the CEM’s prescribed capacities.
2.	Deactivate capacity from the PCM’s generators pursuant to the prescribed
	capacities.
3.	Rank the PCM’s nodes by build favorability.
4.	Add new capacity to the PCM’s nodes pursuant to the prescribed capacities.

The inputs required to run the workflow are the following:
- PCM – Either a Sienna model (`.json` file) or a Plexos model (`.xml` file)
- CEM results – ReEDS run folder
- Node metadata file – A `.csv` file giving information about the PCM’s nodes
  such as their locations, voltages, and load participation factors (required
  columns: `node_id`, `latitude`, `longitude`, `reeds_ba`, `voltage`,
  `load_participation_factor`)

## Process

The following are the steps of the zonal-to-nodal workflow and their corresponding files:
1. Parse CEM results (`parser/reeds.py`)
2. Read node metadata (`parser/nodes.py`)
3. Parse PCM (`parser/plexos.py` or `parser/sienna.py`)
4. Apply CEM results to PCM (`nodal/apply_cem_to_pcm.py`)
	- Compute required capacity additions and reductions to reconcile PCM and
	  CEM zone-wise and technology-wise capacities
	  (`nodal/compute_capacity_additions_and_reductions.py`)
	- This requires a mapping between the PCM and CEM technologies and a
	  mapping connecting the PCM nodes to their CEM zones (from the node
	  metadata file)
	- Execute required capacity reductions by fully and partially deactivating
	  capacity from PCM generators (`nodal/execute_retirements.py`)
		- Higher heat rates correspond to higher deactivation priority
	- Rank PCM nodes (within their zones) by build favorability
	  (`nodal/node_ranking.py`)
		- Higher deactivated capacity, voltage, and load participation factor
		  correspond to higher favorability
		- The available transmission capacity of each node is also computed at
		  this step
	- Execute standard capacity additions by assigning (disaggregated) capacity
	  to PCM nodes (`nodal/execute_capacity_additions.py`)
		- reV technologies and technologies included in enabled custom
		  buildouts (see "user-configurable values") are excluded
		- Capacity is disaggregated according to per-technology average
		  generator sizes from {term}`WECC` and/or according to median generator sizes
		  of the PCM
		- Limits on per-node capacity injection can be enforced with respect to
		  the computed available capacities or user-defined limits (see
		  "user-configurable values")
		- For nodes with deactivated capacity, the amount of deactivate
		  capacity also serves as an upper bound for capacity injection
	- Execute reVX-based capacity additions (`nodal/revx_aggregation.py`)
		- The per-supply curve capacities are normalized according to the
		  investment capacity determined in the first step
		- Only reV technologies (see "user-configurable values") are included
		- Nodal injection limits cannot be enforced
	- Add timeseries data to non-reV VRE technologies (`nodal/postprocessing.py`)
		- Profiles are based on the `recf.h5` input file from the ReEDS run
	- Execute custom capacity additions (`nodal/ext/build_vre_based_bess.py`)
		- This entails assigning new capacity to nodes in ways that deviate
		  from the standard procedure of the fourth step
		- Custom buildouts and their corresponding technologies are defined in
		  nodal_defaults.json (see "user-configurable values")
		- For instance, the "`vre_based_bess`" custom buildout assigns new {term}`BESS`
		  capacity  to PCM nodes with higher VRE capacity, load participation
		  factor, and voltage corresponding to higher favorability and with a
		  maximum of 50% of the node’s solar (if short-duration BESS) or wind
		  (if long-duration BESS) instset guicursor=a:blinkon1alled capacity
	- Add auxiliary generator attributes to the new builds based on ReEDS and
	  WECC data (`nodal/postprocessing.py`)
	- Create formatted generator and storage data (`nodal/postprocessing.py`)
	- Write system update report detailing initial, deactivated, new, and total
	  per-zone and per-technology capacities (`nodal/postprocessing.py`)
-	Export updated PCM (`parser/sienna.py` or `parser/plexos.py`)

## User-configurable Values

- `bess_lpf_threshold` – Only nodes with load participation factor equal to or
  above this threshold will be considered for VRE-based BESS buildout.
  (default: 0.001)
- `custom_nodal_buildout_tech_map` – Mapping between available custom buildouts
  and their corresponding CEM technologies. (default: {`vre_based_bess`:
  [battery]})
- `custom_nodal_buildouts` – Enabled custom buildouts. (default:
  [vre_based_bess])
- `disable_transmission_capacity_limits` – Whether the capacity injection of
  nodes should be limited by their available transmission capacities in
  non-reVX buildouts. (default: {python}`True`)
- `distpv_capacity_threshold_MW` – New distributed PV generators will only be
  assigned capacities equal to or above this value. (default: 100)
- `new_build_prefix` – String that gets attached to the beginning of new
  generators’ names. (default: ReEDS)
- `nodal_build_capacity_limit_MW` – Each node can only receive new capacity until
  its total capacity reaches this value. (default: infinity)
- `nodal_build_unit_limit` – Each node can only receive new capacity until its
  total number of units reaches this value. (default: 5)
- `reV_capacity_threshold_MW` – Minimum capacity of new generators created by
  reVX. (default: 100)
- `reV_node_voltage_threshold_kV` – Only nodes with voltages equal to or above
  this threshold will be considered for the buildout of reV technologies
  (rev_techs). (default: 100)
- `rev_techs` – CEM technologies that should be built by reVX PlexosAggregation.
  You must have reV supply curves and capacity factor files for each tech.
  (default: [upv, wind-ons, wind-ofs])
