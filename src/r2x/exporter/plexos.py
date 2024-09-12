"""Create PLEXOS model from translated ReEDS data."""

from typing import Any
import uuid
import string
from collections.abc import Callable

from infrasys.component import Component
from loguru import logger

from r2x.enums import ReserveType
from r2x.exporter.handler import BaseExporter
from plexosdb import PlexosSQLite
from plexosdb.enums import ClassEnum, CollectionEnum
from r2x.models import (
    ACBus,
    Emission,
    InterruptiblePowerLoad,
    Generator,
    GenericBattery,
    HydroDispatch,
    HydroEnergyReservoir,
    HydroPumpedStorage,
    LoadZone,
    MonitoredLine,
    PowerLoad,
    RenewableDispatch,
    RenewableNonDispatch,
    Reserve,
    ThermalStandard,
    Transformer2W,
    TransmissionInterface,
)
from r2x.units import get_magnitude
from r2x.utils import custom_attrgetter, get_enum_from_string, read_json, get_property_magnitude

NESTED_ATTRIBUTES = ["ext", "bus", "services"]
TIME_SERIES_PROPERTIES = ["Min Provision", "Static Risk"]


class PlexosExporter(BaseExporter):
    """Plexos exporter class."""

    def __init__(
        self,
        *args,
        plexos_scenario: str = "default",
        database_manager=None,
        xml_fname: str | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.plexos_scenario = plexos_scenario
        self.property_map = self.config.defaults["plexos_property_map"]
        self.valid_properties = self.config.defaults["valid_properties"]
        self.default_units = self.config.defaults["default_units"]

        self._db_mgr = database_manager or PlexosSQLite(xml_fname=xml_fname)
        self.plexos_scenario_id = self._db_mgr.get_scenario_id(scenario_name=plexos_scenario)

    def run(self, *args, new_database: bool = True, **kwargs) -> "PlexosExporter":
        """Run the exporter."""
        logger.info("Starting {}", self.__class__.__name__)

        self.export_data_files()

        # If starting w/o a reference file we add our custom models and objects
        if new_database:
            self._add_simulation_objects()
            self._add_horizons()
            self._add_models()
            self._add_reports()
        self.add_topology()
        self.add_lines()
        self.add_transformers()
        self.add_interfaces()
        self.add_emissions()
        self.add_reserves()
        self.add_batteries()
        self.add_generators()
        self.add_storage()

        self._db_mgr.to_xml(fpath=f"{self.output_folder}/{self.config.name}.xml")

        return self

    def _get_time_series_properties(self, component):  # noqa: C901
        """Add time series object to certain plexos properties."""
        if not self.system.has_time_series(component):
            return

        ts_metadata = self.system.get_time_series(component)

        config_dict = self.config.__dict__
        config_dict["name"] = self.config.name
        csv_fname = config_dict.get("time_series_fname", "${component_type}_${name}_${weather_year}.csv")
        string_template = string.Template(csv_fname)
        config_dict["component_type"] = f"{component.__class__.__name__}_{ts_metadata.variable_name}"
        csv_fname = string_template.safe_substitute(config_dict)
        csv_fpath = self.ts_directory / csv_fname
        time_series_property: dict[str, Any] = {"Data File": str(csv_fpath)}

        # Property with time series can change. Validate this option based on the component type.
        match component:
            case InterruptiblePowerLoad():
                time_series_property["Fixed Load"] = "0"
            case PowerLoad():
                time_series_property["Load"] = "0"
            case RenewableDispatch():
                time_series_property["Rating"] = "0"
                time_series_property["Load Subtracter"] = "0"
            case RenewableNonDispatch():
                time_series_property["Rating"] = "0"
                time_series_property["Load Subtracter"] = "0"
            case Reserve():
                match component.reserve_type:
                    case ReserveType.SPINNING:
                        time_series_property["Min Provision"] = "0"
                    case ReserveType.FLEXIBILITY:
                        time_series_property["Min Provision"] = "0"
                    case ReserveType.REGULATION:
                        time_series_property["Static Risk"] = "0"
                    case _:
                        raise NotImplementedError(f"Reserve {component.type} not supported")
            case HydroDispatch():
                time_series_property["Max Energy"] = "0"
            case HydroEnergyReservoir():
                time_series_property["Fixed Load"] = "0"
            case ThermalStandard():
                variable_name = self.system.get_time_series(component).variable_name
                if not variable_name:
                    return None
                property_name = self.property_map.get(variable_name, None)
                if property_name:
                    time_series_property[property_name] = "0"
            case _:
                raise NotImplementedError(f"Time Series for {component.label} not supported yet.")
        return time_series_property

    def insert_component_properties(
        self,
        component_type: type["Component"],
        /,
        *,
        parent_class: ClassEnum,
        parent_object_name: str = "System",
        collection: CollectionEnum,
        filter_func: Callable | None = None,
        scenario: str | None = None,
        records: list[dict] | None = None,
        exclude_fields: list[str] = NESTED_ATTRIBUTES,
    ) -> None:
        """Bulk insert properties from selected component type."""
        logger.debug("Adding {} table properties...", component_type.__name__)
        scenario = scenario or self.plexos_scenario
        if not records:
            records = [
                component.model_dump(exclude_none=True, exclude=exclude_fields)
                for component in self.system.get_components(component_type, filter_func=filter_func)
            ]

        if not records:
            logger.warning("No components found for type {}", component_type)
            return

        collection_properties = self._db_mgr.query(
            f"select name, property_id from t_property where collection_id={collection}"
        )
        property_names = [key[0] for key in collection_properties]
        match component_type.__name__:
            case "GenericBattery":
                custom_map = {"base_power": "Max Power", "storage_capacity": "Capacity"}
            case _:
                custom_map = {}
        property_map = self.property_map | custom_map
        valid_component_properties = self.get_valid_records_properties(
            records,
            property_map,
            self.default_units,
            valid_properties=property_names,
        )
        self._db_mgr.add_property_from_records(
            valid_component_properties,
            parent_class=parent_class,
            parent_object_name=parent_object_name,
            collection=collection,
            scenario=scenario,
        )

    def add_component_category(
        self,
        component_type: type["Component"],
        class_enum: ClassEnum,
        category_attribute: str = "category",
        filter_func: Callable | None = None,
    ):
        """Add all categories for a component type."""
        component_categories = {
            custom_attrgetter(component, category_attribute)
            for component in self.system.get_components(component_type, filter_func=filter_func)
        }

        existing_rank = self._db_mgr.query(f"select max(rank) from t_category where class_id = {class_enum}")[
            0
        ][0]
        categories = [
            (class_enum.value, rank, category or "")
            for rank, category in enumerate(sorted(component_categories), start=existing_rank + 1)
        ]

        # Maybe replace `t_category` with right schema.
        with self._db_mgr._conn as conn:
            conn.executemany("INSERT into t_category(class_id, rank, name) values (?,?,?)", categories)
        return

    def bulk_insert_objects(
        self,
        component_type: type["Component"],
        class_enum: ClassEnum,
        collection_enum: CollectionEnum,
        category_attribute: str = "category",
        name_prefix: str = "",
        filter_func: Callable | None = None,
    ):
        """Bulk insert objects to the database."""
        logger.debug("Adding plexos objects for component type {}", component_type.__name__)
        categories_ids = {
            key: value
            for key, value in self._db_mgr.query(
                f"select name, category_id from t_category where class_id = {class_enum}"
            )
        }

        objects = [
            (
                class_enum.value,
                component.name + name_prefix,
                self._get_category_id(component, category_attribute, categories_ids),
                str(uuid.uuid4()),
            )
            for component in self.system.get_components(component_type, filter_func=filter_func)
        ]
        object_names = tuple(d[1] for d in objects)
        if not objects:
            logger.warning("No components found for type {}", component_type)
            return
        with self._db_mgr._conn as conn:
            conn.executemany(
                "INSERT into t_object(class_id, name, category_id, GUID) values (?,?,?,?)", objects
            )

        # Add system membership
        system_object_id: int = self._db_mgr.query("select object_id from t_object where name = 'System'")[0][
            0
        ]
        objects_placeholders = ", ".join("?" * len(object_names))
        membership_query = f"""
            INSERT into t_membership(
              parent_class_id, parent_object_id,
              collection_id, child_class_id, child_object_id
            )
            SELECT
                ? as parent_class_id,
                ? as parent_object_id,
                ? as collection_id,
                ? as child_class_id,
                object_id as child_object_id
            FROM
              t_object
            WHERE
              class_id = ?
              and t_object.name in ({objects_placeholders})
        """
        query_parameters = (
            ClassEnum.System.value,
            system_object_id,
            str(collection_enum),
            class_enum,
            class_enum,
            *object_names,
        )
        self._db_mgr.execute_query(membership_query, query_parameters)

        # Enable all classes that we add into the database.
        # Some are disabled by default.
        self._db_mgr.execute_query(f"UPDATE t_class set is_enabled=1 where class_id={class_enum}")
        return

    def add_topology(self) -> None:
        """Create network topology on Plexos."""
        logger.debug("Adding plexos objects")

        # Adding Regions
        self.add_component_category(ACBus, category_attribute="area.name", class_enum=ClassEnum.Region)
        self.bulk_insert_objects(
            ACBus,
            category_attribute="area.name",
            class_enum=ClassEnum.Region,
            collection_enum=CollectionEnum.SystemRegions,
        )
        self.insert_component_properties(
            ACBus, parent_class=ClassEnum.System, collection=CollectionEnum.SystemRegions
        )

        # Adding Zones
        # self.add_component_category(LoadZone, class_enum=ClassEnum.Zone)
        self.bulk_insert_objects(
            LoadZone, class_enum=ClassEnum.Zone, collection_enum=CollectionEnum.SystemZones
        )
        self.insert_component_properties(
            LoadZone, parent_class=ClassEnum.System, collection=CollectionEnum.SystemZones
        )

        # Adding nodes
        # NOTE: For nodes, we do not add category.
        # self.add_component_category(ACBus, class_enum=ClassEnum.Node)
        self.bulk_insert_objects(ACBus, class_enum=ClassEnum.Node, collection_enum=CollectionEnum.SystemNodes)
        self.insert_component_properties(
            ACBus, parent_class=ClassEnum.System, collection=CollectionEnum.SystemNodes
        )

        # Add node memberships to zone and regions.
        # On our default Plexos translation, both Zones and Regions are child of the Node class.
        for bus in self.system.get_components(ACBus):
            self._db_mgr.add_membership(
                bus.name,
                bus.name,  # Zone has the same name
                parent_class=ClassEnum.Node,
                child_class=ClassEnum.Region,
                collection=CollectionEnum.NodesRegion,
            )
            self._db_mgr.add_membership(
                bus.name,
                bus.load_zone.name,
                parent_class=ClassEnum.Node,
                child_class=ClassEnum.Zone,
                collection=CollectionEnum.NodesZone,
            )

        # Adding load time series
        logger.debug("Adding load time series properties")
        for component in self.system.get_components(
            PowerLoad, filter_func=lambda component: self.system.has_time_series(component)
        ):
            time_series_properties = self._get_time_series_properties(component)
            if time_series_properties:
                text = time_series_properties.pop("Data File")
                for property_name, property_value in time_series_properties.items():
                    self._db_mgr.add_property(
                        component.bus.name,
                        property_name,
                        property_value,
                        object_class=ClassEnum.Region,
                        collection=CollectionEnum.SystemRegions,
                        scenario=self.plexos_scenario,
                        text={"Data File": text},
                    )
        return

    def add_lines(self) -> None:
        """Add transmission lines that connect topology elements."""
        # Adding Lines
        # NOTE: The default line on Plexos is a `MonitoredLine` without category. If we need to add a category
        # in the future, we will uncomment the line below with the pertinent category name.
        # self.add_component_category(MonitoredLine, class_enum=ClassEnum.Line)
        self.bulk_insert_objects(
            MonitoredLine, class_enum=ClassEnum.Line, collection_enum=CollectionEnum.SystemLines
        )
        self.insert_component_properties(
            MonitoredLine, parent_class=ClassEnum.System, collection=CollectionEnum.SystemLines
        )

        for line in self.system.get_components(MonitoredLine):
            properties = self.get_valid_component_properties(
                line.ext,
                property_map=self.property_map,
                unit_map=self.default_units,
                collection=CollectionEnum.SystemLines,
            )
            for property_name, property_value in properties.items():
                self._db_mgr.add_property(
                    line.name,
                    property_name,
                    property_value,
                    object_class=ClassEnum.Line,
                    collection=CollectionEnum.SystemLines,
                    scenario=self.plexos_scenario,
                )
            self._db_mgr.add_membership(
                line.name,
                line.from_bus.name,
                parent_class=ClassEnum.Line,
                child_class=ClassEnum.Node,
                collection=CollectionEnum.LineNodeFrom,
            )
            self._db_mgr.add_membership(
                line.name,
                line.to_bus.name,
                parent_class=ClassEnum.Line,
                child_class=ClassEnum.Node,
                collection=CollectionEnum.LineNodeTo,
            )
        return

    def add_transformers(self) -> None:
        """Add Transformer objects to the database."""
        self.add_component_category(Transformer2W, class_enum=ClassEnum.Transformer)
        self.bulk_insert_objects(
            Transformer2W, class_enum=ClassEnum.Transformer, collection_enum=CollectionEnum.SystemTransformers
        )
        self.insert_component_properties(
            Transformer2W, parent_class=ClassEnum.System, collection=CollectionEnum.SystemTransformers
        )
        for transformer in self.system.get_components(Transformer2W):
            self._db_mgr.add_membership(
                transformer.name,
                transformer.from_bus.name,
                parent_class=ClassEnum.Transformer,
                child_class=ClassEnum.Node,
                collection=CollectionEnum.TransformerNodeFrom,
            )
            self._db_mgr.add_membership(
                transformer.name,
                transformer.to_bus.name,
                parent_class=ClassEnum.Transformer,
                child_class=ClassEnum.Node,
                collection=CollectionEnum.TransformerNodeTo,
            )
        return

    def add_interfaces(self) -> None:
        """Add transmission interfaces."""
        self.bulk_insert_objects(
            TransmissionInterface,
            class_enum=ClassEnum.Interface,
            collection_enum=CollectionEnum.SystemInterfaces,
        )
        self.insert_component_properties(
            TransmissionInterface, parent_class=ClassEnum.System, collection=CollectionEnum.SystemInterfaces
        )

    def add_emissions(self) -> None:
        """Add emission objects to the database."""
        # Getting all unique emission types (e.g., CO2, NOX) from the emissions objects.
        # NOTE: On Plexos, we need to add each emission type individually to the Emission class
        self._db_mgr.execute_query(
            f"UPDATE t_class set is_enabled=1 where class_id={ClassEnum.Emission.value}"
        )
        emission_types = set(map(lambda x: x.emission_type, list(self.system.get_components(Emission))))
        for emission_type in emission_types:
            self._db_mgr.add_object(
                emission_type,
                ClassEnum.Emission,
                CollectionEnum.SystemEmissions,
            )

        return

    def add_reserves(self) -> None:
        """Add system reserves to the database.

        This method only appends the information for the type of reserves. Each
        generator that contributes to the reserve will add the membership for
        the type of reserve as a child membership.
        """
        # Adding reserves
        # self.add_component_category(Reserve, class_enum=ClassEnum.Reserve)
        self.bulk_insert_objects(
            Reserve, class_enum=ClassEnum.Reserve, collection_enum=CollectionEnum.SystemReserves
        )

        self.insert_component_properties(
            Reserve,
            parent_class=ClassEnum.System,
            collection=CollectionEnum.SystemReserves,
            exclude_fields=[*NESTED_ATTRIBUTES, "max_requirement"],
        )
        for reserve in self.system.get_components(Reserve):
            properties = {}
            properties["Type"] = reserve.ext["Type"]
            properties["Is Enabled"] = "-1" if reserve.available else "0"
            properties["Mutually Exclusive"] = True

            for property, value in properties.items():
                self._db_mgr.add_property(
                    reserve.name,
                    property,
                    value,
                    object_class=ClassEnum.Reserve,
                    collection=CollectionEnum.SystemReserves,
                    scenario=self.plexos_scenario,
                )
            time_series_properties = self._get_time_series_properties(reserve)
            if time_series_properties:
                text = time_series_properties.pop("Data File")
                for property_name, property_value in time_series_properties.items():
                    self._db_mgr.add_property(
                        reserve.name,
                        property_name,
                        property_value,
                        object_class=ClassEnum.Reserve,
                        collection=CollectionEnum.SystemReserves,
                        scenario=self.plexos_scenario,
                        text={"Data File": text},
                    )

            # Add ReserveRegions properties. Currently, we only add the load_risk
            component_dict = reserve.model_dump(
                exclude_none=True, exclude=[*NESTED_ATTRIBUTES, "max_requirement"]
            )

            regions = self.system.get_components(
                ACBus, filter_func=lambda x: x.load_zone.name == reserve.region.name
            )

            for region in regions:
                self._db_mgr.add_membership(
                    reserve.name,
                    region.name,  # Zone has the same name
                    parent_class=ClassEnum.Reserve,
                    child_class=ClassEnum.Region,
                    collection=CollectionEnum.ReserveRegions,
                )
                properties = self.get_valid_component_properties(
                    component_dict,
                    property_map=self.property_map,
                    unit_map=self.default_units,
                    collection=CollectionEnum.ReserveRegions,
                )
                if properties:
                    for property_name, property_value in properties.items():
                        self._db_mgr.add_property(
                            region.name,
                            property_name,
                            property_value,
                            object_class=ClassEnum.Region,
                            parent_object_name=reserve.name,
                            parent_class=ClassEnum.Reserve,
                            collection=CollectionEnum.ReserveRegions,
                            scenario=self.plexos_scenario,
                        )
        return

    def add_generators(self):
        """Add generator objects to the database."""

        # Add generator objects excluding batteries
        def exclude_battery(component):
            return not isinstance(component, GenericBattery)

        self.add_component_category(Generator, class_enum=ClassEnum.Generator, filter_func=exclude_battery)
        self.bulk_insert_objects(
            Generator,
            class_enum=ClassEnum.Generator,
            collection_enum=CollectionEnum.SystemGenerators,
            filter_func=exclude_battery,
        )
        self.insert_component_properties(
            Generator,
            parent_class=ClassEnum.System,
            collection=CollectionEnum.SystemGenerators,
            filter_func=exclude_battery,
        )

        # Add generator memberships
        logger.debug("Adding generator memberships")
        for generator in self.system.get_components(Generator, filter_func=exclude_battery):
            self._db_mgr.add_membership(
                generator.name,
                generator.bus.name,
                parent_class=ClassEnum.Generator,
                child_class=ClassEnum.Node,
                collection=CollectionEnum.GeneratorNodes,
            )
            properties = self._get_time_series_properties(generator)
            if properties:
                text = properties.pop("Data File")
                for property_name, property_value in properties.items():
                    self._db_mgr.add_property(
                        generator.name,
                        property_name,
                        property_value,
                        object_class=ClassEnum.Generator,
                        collection=CollectionEnum.SystemGenerators,
                        scenario=self.plexos_scenario,
                        text={"Data File": text},
                    )
            if generator.services:
                for service in generator.services:
                    match service:
                        case Reserve():
                            self._db_mgr.add_membership(
                                service.name,
                                generator.name,
                                parent_class=ClassEnum.Reserve,
                                child_class=ClassEnum.Generator,
                                collection=CollectionEnum.ReserveGenerators,
                            )
                        case _:
                            raise NotImplementedError(f"{service} not yet implemented for generator.")

        # NOTE: This needs to be optimized. It is currently slow.
        logger.debug("Adding generator emisssions memberships")
        for emission in self.system.get_components(Emission):
            self._db_mgr.add_membership(
                emission.emission_type,
                emission.generator_name,
                parent_class=ClassEnum.Emission,
                child_class=ClassEnum.Generator,
                collection=CollectionEnum.EmissionGenerators,
            )
            self._db_mgr.add_property(
                emission.generator_name,
                self.property_map["rate"],
                get_magnitude(emission.rate),
                object_class=ClassEnum.Generator,
                parent_class=ClassEnum.Emission,
                parent_object_name=emission.emission_type,
                collection=CollectionEnum.EmissionGenerators,
                scenario=self.plexos_scenario,
            )

    def add_batteries(self):
        """Add battery objects to the database."""
        # Add battery objects
        self.add_component_category(GenericBattery, class_enum=ClassEnum.Battery)
        self.bulk_insert_objects(
            GenericBattery,
            class_enum=ClassEnum.Battery,
            collection_enum=CollectionEnum.SystemBattery,
        )
        self.insert_component_properties(
            GenericBattery, parent_class=ClassEnum.System, collection=CollectionEnum.SystemBattery
        )
        # Add battery memberships
        for battery in self.system.get_components(GenericBattery):
            self._db_mgr.add_membership(
                battery.name,
                battery.bus.name,
                parent_class=ClassEnum.Battery,
                child_class=ClassEnum.Node,
                collection=CollectionEnum.BatteryNodes,
            )
            if battery.services:
                for service in battery.services:
                    match service:
                        case Reserve():
                            self._db_mgr.add_membership(
                                service.name,
                                battery.name,
                                parent_class=ClassEnum.Reserve,
                                child_class=ClassEnum.Battery,
                                collection=CollectionEnum.ReserveBatteries,
                            )
                        case _:
                            raise NotImplementedError(f"{service} not yet implemented for generator.")

    def add_storage(self):
        """Add storage objects to the database."""
        # Add pump storage objects
        self.add_component_category(
            HydroPumpedStorage, category_attribute="head", class_enum=ClassEnum.Storage
        )
        self.add_component_category(
            HydroPumpedStorage, category_attribute="tail", class_enum=ClassEnum.Storage
        )
        self.bulk_insert_objects(
            HydroPumpedStorage,
            class_enum=ClassEnum.Storage,
            category_attribute="head",
            collection_enum=CollectionEnum.SystemStorage,
            name_prefix="_head",
        )
        self.bulk_insert_objects(
            HydroPumpedStorage,
            class_enum=ClassEnum.Storage,
            category_attribute="tail",
            collection_enum=CollectionEnum.SystemStorage,
            name_prefix="_tail",
        )

        head_storage = [
            component.model_dump(exclude_none=True, exclude=NESTED_ATTRIBUTES)
            for component in self.system.get_components(HydroPumpedStorage)
        ]
        for component in head_storage:
            component["name"] += "_head"
        self.insert_component_properties(
            HydroPumpedStorage,
            parent_class=ClassEnum.System,
            collection=CollectionEnum.SystemStorage,
            records=head_storage,
        )
        tail_storage = [
            component.model_dump(exclude_none=True, exclude=NESTED_ATTRIBUTES)
            for component in self.system.get_components(HydroPumpedStorage)
        ]
        for component in tail_storage:
            component["name"] += "_tail"
        self.insert_component_properties(
            HydroPumpedStorage,
            parent_class=ClassEnum.System,
            collection=CollectionEnum.SystemStorage,
            records=tail_storage,
        )

        for phs in self.system.get_components(HydroPumpedStorage):
            head_name = f"{phs.name}_head"
            tail_name = f"{phs.name}_tail"
            self._db_mgr.add_membership(
                phs.name,
                head_name,
                parent_class=ClassEnum.Generator,
                child_class=ClassEnum.Storage,
                collection=CollectionEnum.GeneratorHeadStorage,
            )
            self._db_mgr.add_membership(
                phs.name,
                tail_name,
                parent_class=ClassEnum.Generator,
                child_class=ClassEnum.Storage,
                collection=CollectionEnum.GeneratorTailStorage,
            )
        return

    def _add_simulation_objects(self):
        for simulation_object in self.config.defaults["simulation_objects"]:
            collection_enum = get_enum_from_string(simulation_object["name"], CollectionEnum, prefix="System")
            class_enum: ClassEnum = get_enum_from_string(simulation_object["name"], ClassEnum)
            for objects in simulation_object["attributes"]:
                self._db_mgr.add_object(
                    objects["name"],
                    class_enum,
                    collection_enum,
                    category_name=objects["category"] or "-",
                )

            # Add properties
            property_list = self.config.defaults[simulation_object["name"]]
            for attributes in property_list["attributes"]:
                self._db_mgr.add_attribute(
                    object_name=attributes["name"],
                    object_class=get_enum_from_string(attributes["class"], ClassEnum),
                    attribute_class=class_enum,
                    attribute_name=attributes["attribute"],
                    attribute_value=attributes["value"],
                )

    def _add_horizons(self):
        logger.info("Adding model horizon")
        static_horizon_type = self.config.defaults["static_horizon_type"]
        static_horizons = self.config.defaults[static_horizon_type]
        for horizon, values in static_horizons.items():
            self._db_mgr.add_object(
                horizon,
                ClassEnum.Horizon,
                CollectionEnum.SystemHorizon,
            )
            for attribute, attribute_value in values["attributes"].items():
                self._db_mgr.add_attribute(
                    object_name=horizon,
                    object_class=ClassEnum.Horizon,
                    attribute_class=ClassEnum.Horizon,
                    attribute_name=attribute,
                    attribute_value=attribute_value,
                )

    def _add_models(self):
        static_model_type = self.config.defaults["static_model_type"]
        static_models = self.config.defaults[static_model_type]
        for model, values in static_models.items():
            self._db_mgr.add_object(
                model,
                ClassEnum.Model,
                CollectionEnum.SystemModel,
                category_name=values["category"],
            )
            for attribute, attribute_value in values["attributes"].items():
                self._db_mgr.add_attribute(
                    object_name=model,
                    object_class=ClassEnum.Model,
                    attribute_class=ClassEnum.Model,
                    attribute_name=attribute,
                    attribute_value=attribute_value,
                )
            for child_class_name, child_object_name in values["memberships"].items():
                collection_enum = get_enum_from_string(child_class_name, CollectionEnum, prefix="Model")
                child_class = get_enum_from_string(child_class_name, ClassEnum)
                self._db_mgr.add_membership(
                    model,
                    child_object_name,
                    parent_class=ClassEnum.Model,
                    child_class=child_class,
                    collection=collection_enum,
                )
            self._db_mgr.add_membership(
                model,
                self.plexos_scenario,
                parent_class=ClassEnum.Model,
                child_class=ClassEnum.Scenario,
                collection=CollectionEnum.ModelScenario,
            )
        return

    def _add_reports(self):
        fpath = self.config.defaults["plexos_reports"]
        report_objects = read_json(fpath)

        for report_object in report_objects:
            self._db_mgr.add_report(**report_object)
        return

    def _get_category_id(self, component, category_attribute, categories_ids, default_category="-"):
        category_to_get = custom_attrgetter(component, category_attribute)

        if category_to_get not in categories_ids:
            return categories_ids[default_category]
        return categories_ids[category_to_get]

    def get_valid_component_properties(
        self,
        component_dict: dict,
        property_map: dict[str, str],
        unit_map: dict[str, str],
        collection: CollectionEnum,
    ):
        """Validate single component properties."""
        valid_component_properties = {}
        component_dict_mapped = {property_map.get(key, key): value for key, value in component_dict.items()}
        collection_properties = self._db_mgr.query(
            f"select name, property_id from t_property where collection_id={collection}"
        )
        valid_properties = [key[0] for key in collection_properties]
        for property_name, property_value in component_dict_mapped.items():
            if property_name in valid_properties:
                property_value = get_property_magnitude(property_value, to_unit=unit_map.get(property_name))
                valid_component_properties[property_name] = property_value
        return valid_component_properties
