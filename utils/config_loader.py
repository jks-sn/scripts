#utils/config_loader.py

import os
import json

def load_config():
    """Загрузка конфигурации из config.json."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    try:
        with open(config_path) as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Ошибка в формате JSON файла: {config_path}")

def get_clusters_dict():
    """Преобразование списка кластеров в словарь по имени."""
    config = load_config()
    return {cluster['name']: cluster for cluster in config['clusters']}