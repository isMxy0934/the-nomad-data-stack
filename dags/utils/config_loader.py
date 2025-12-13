import yaml
import os

def load_metadata_config(target_source: str):
    """
    Load metadata configuration from YAML file
    Args:
        target_source: source of the metadata
    Returns:
        list: filtered metadata configuration
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    yaml_path = os.path.join(base_dir, 'config', 'metadata_config.yaml')
    
    with open(yaml_path, 'r', encoding='utf-8') as f:
        all_items = yaml.safe_load(f)
        
    filtered_items = [item for item in all_items if item.get('source') == target_source]
    
    print(f"[{target_source}] loaded metadata configuration: total {len(all_items)}, hit {len(filtered_items)}")
    return filtered_items