import importlib
from typing import Any, Type

def load_class(class_path: str) -> Type[Any]:
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
        raise ImportError(f"Could not load class {class_path}: {e}")

def instantiate_component(config: dict) -> Any:
    """
    Instantiates a component from a config dictionary.
    Config must have a 'class' key, and optional 'kwargs'.
    """
    cls_path = config.get("class")
    if not cls_path:
        raise ValueError("Component config missing 'class' field")
    
    cls = load_class(cls_path)
    kwargs = config.get("kwargs", {})
    
    # Recursively instantiate nested components (e.g. for CompositePartitioner)
    # If a kwarg value is a list of dicts with 'class', instantiate them too.
    if "strategies" in kwargs and isinstance(kwargs["strategies"], list):
        strategies = []
        for strat_conf in kwargs["strategies"]:
            if isinstance(strat_conf, dict) and "class" in strat_conf:
                strategies.append(instantiate_component(strat_conf))
        kwargs["strategies"] = strategies

    return cls(**kwargs)
