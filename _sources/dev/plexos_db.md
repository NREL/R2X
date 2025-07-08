# Plexos API design

## How to perform bulk inserts for a component

If you have a system with multiple components that contains multiple fields it is faster to perform a bulk insert instead of a regular {meth}`.add_category`.

```python
def insert_components_properties(
    self,
    component_type: Type["Component"],
    /,
    *,
    parent_class: ClassEnum,
    parent_object_name: str = "System",
    collection_enum: CollectionEnum,
    filter_func: Callable | None = None,
    scenario: str | None = None,
) -> None: ...
```

<!-- 1. Get list of all properties to add for a given generator as list of dicts, -->
<!-- 2. Get all the memberships of the object that requires insert -->
<!--     - By default, the `parent_class` is assumed to be the system, but we can change it by passing both `parent_class` and `parent_object_name` -->
<!--     - It depends on a main query that matches the column `t_membership.child_object_id` with all the object name for the component provided (see below snipped). The last condition `t_object.name in (?,?,?)` is dynamically created base on the number of objects found for the given component. -->
<!---->
<!--     ```sql -->
<!--     SELECT -->
<!--         t_object.name as name, -->
<!--         membership_id -->
<!--     FROM -->
<!--         t_membership -->
<!--         inner join t_object on t_membership.child_object_id = t_object.object_id -->
<!--     WHERE -->
<!--         t_membership.parent_object_id = {parent_object_id} and -->
<!--         t_object.name in (?,?,?) -->
<!--     ``` -->

## How to perform bulk inserts

```python
def insert_components_properties(
    self,
    component_type: Type["Component"],
    /,
    *,
    parent_class: ClassEnum,
    parent_object_name: str = "System",
    collection_enum: CollectionEnum,
    filter_func: Callable | None = None,
    scenario: str | None = None,
) -> None: ...
```
