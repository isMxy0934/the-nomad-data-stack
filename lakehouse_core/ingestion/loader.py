from __future__ import annotations

import importlib
from typing import Any


def load_class(class_path: str) -> type[Any]:
    """Load a class from a dotted path."""
    if "." not in class_path:
        raise ValueError(f"Invalid class path: {class_path}")
    module_name, class_name = class_path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except (ImportError, AttributeError) as exc:
        raise ImportError(f"Could not load class {class_path}: {exc}") from exc


def _is_component_config(value: Any) -> bool:
    return isinstance(value, dict) and "class" in value


def _instantiate_nested_components(kwargs: dict) -> dict:
    result = {}
    for key, value in kwargs.items():
        if _is_component_config(value):
            result[key] = instantiate_component(value)
        elif isinstance(value, list):
            result[key] = [
                instantiate_component(item) if _is_component_config(item) else item
                for item in value
            ]
        elif isinstance(value, dict):
            result[key] = _instantiate_nested_components(value)
        else:
            result[key] = value
    return result


def instantiate_component(config: dict) -> Any:
    cls_path = config.get("class")
    if not cls_path:
        raise ValueError("Component config missing 'class' field")
    cls = load_class(cls_path)
    kwargs = config.get("kwargs", {})
    instantiated_kwargs = _instantiate_nested_components(kwargs)
    return cls(**instantiated_kwargs)
