import importlib
from typing import Any


def load_class(class_path: str) -> type[Any]:
    """
    Dynamically loads a class from a string path.
    e.g., "dags.ingestion.standard.partitioners.SqlPartitioner"
    """
    if "." not in class_path:
        raise ValueError(f"Invalid class path: {class_path}")

    module_name, class_name = class_path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        return cls
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Could not load class {class_path}: {e}") from e


def _is_component_config(value: Any) -> bool:
    """Check if a value is a component configuration dict.

    A component config is a dict with a 'class' key.

    Args:
        value: Value to check

    Returns:
        True if value is a component config dict
    """
    return isinstance(value, dict) and "class" in value


def _instantiate_nested_components(kwargs: dict) -> dict:
    """Recursively instantiate nested components in kwargs.

    This function handles:
    - Single component configs: {"class": "...", "kwargs": {...}}
    - Lists of component configs: [{"class": "..."}, {"class": "..."}]
    - Nested structures: {"strategies": [{"class": "...", "kwargs": {"inner": {"class": "..."}}}]}

    Args:
        kwargs: Dictionary potentially containing nested component configs

    Returns:
        Dictionary with component configs replaced by instantiated objects
    """
    result = {}

    for key, value in kwargs.items():
        if _is_component_config(value):
            # Single nested component
            result[key] = instantiate_component(value)
        elif isinstance(value, list):
            # List of potential components
            result[key] = [
                instantiate_component(item) if _is_component_config(item) else item
                for item in value
            ]
        elif isinstance(value, dict):
            # Nested dict - recurse but don't instantiate
            result[key] = _instantiate_nested_components(value)
        else:
            # Primitive value
            result[key] = value

    return result


def instantiate_component(config: dict) -> Any:
    """Instantiate a component from a config dictionary with recursive nesting support.

    Config format:
    {
        "class": "module.path.ClassName",
        "kwargs": {
            "param1": "value1",
            "nested_component": {
                "class": "module.path.NestedClass",
                "kwargs": {...}
            },
            "component_list": [
                {"class": "module.path.Class1", "kwargs": {...}},
                {"class": "module.path.Class2", "kwargs": {...}}
            ]
        }
    }

    Args:
        config: Component configuration dictionary

    Returns:
        Instantiated component object

    Raises:
        ValueError: If config is missing 'class' field
        ImportError: If class cannot be loaded
    """
    cls_path = config.get("class")
    if not cls_path:
        raise ValueError("Component config missing 'class' field")

    cls = load_class(cls_path)
    kwargs = config.get("kwargs", {})

    # Recursively instantiate all nested components
    instantiated_kwargs = _instantiate_nested_components(kwargs)

    return cls(**instantiated_kwargs)
