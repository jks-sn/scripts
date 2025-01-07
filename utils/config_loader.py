#utils/config_loader.py

import os
import json
from models.config import Config

def load_config() -> Config:
    """Загрузка конфигурации из config.json."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    try:
        with open(config_path) as f:
            config_data = json.load(f)

        return Config(**config_data)

    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON decoding error in the file {config_path}: {e}")
    except Exception as e:
        raise ValueError(f"An unknown error occurred while loading the configuration: {e}")


def get_clusters_dict() -> dict:
    """Преобразование списка кластеров в словарь по имени."""
    config = load_config()
    return {cluster.name: cluster.model_dump() for cluster in config.clusters}