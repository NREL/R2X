import importlib.metadata
import inspect

def find_subclasses(module_name:str, base_class):
    """
    Find all classes in a module that inherit from a specific base class.

    Args:
        module_name (str): Name of the module to inspect
        base_class (type): Base class to check inheritance against

    Returns:
        list: List of classes that inherit from base_class
    """
    try:
        # Load the module
        module = importlib.import_module(module_name)

        # Get all members of the module that are classes
        classes = [
            obj for name, obj in inspect.getmembers(module, inspect.isclass)
            if obj.__module__ == module_name  # Only include classes defined in this module
        ]

        # Filter for subclasses of base_class
        subclasses = [
            cls for cls in classes
            if issubclass(cls, base_class) and cls != base_class
        ]

        return subclasses

    except ImportError as e:
        print(f"Could not import module {module_name}: {e}")
        return []

# Using with entry points
def find_subclasses_from_entry_points(group_name: str, base_class):
    """
    Find subclasses from entry points with a specific group name.

    Args:
        group_name (str): Entry point group name
        base_class (type): Base class to check inheritance against

    Returns:
        dict: Mapping of entry point names to their matching classes
    """
    results = {}

    # Get all entry points for the specified group
    entry_points = importlib.metadata.entry_points()

    # Handle Python 3.10+ vs older versions
    if hasattr(entry_points, 'select'):  # Python 3.10+
        eps = entry_points.select(group=group_name)

    for ep in eps:
        try:
            # Load the entry point
            loaded = ep.load()

            # If it's a class, check it directly
            if inspect.isclass(loaded):
                if issubclass(loaded, base_class) and loaded != base_class:
                    results[ep.name] = loaded
            # If it's a module, inspect its classes
            elif inspect.ismodule(loaded):
                classes = [
                    obj for name, obj in inspect.getmembers(loaded, inspect.isclass)
                    if issubclass(obj, base_class) and obj != base_class
                ]
                if classes:
                    results[ep.name] = classes

        except Exception as e:
            print(f"Error loading entry point {ep.name}: {e}")

    return results
