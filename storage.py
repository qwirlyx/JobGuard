from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any

STORAGE_VERSION = 1


def _normalize_state(raw: dict[str, Any], default_state: dict[str, Any]) -> dict[str, Any]:
    """Добавляет недостающие ключи, чтобы старый JSON не ломал приложение.

    Важно: ключи берутся из default_state, поэтому новые разделы вроде
    support_tickets не теряются после перезапуска.
    """
    state = deepcopy(default_state)

    for key, default_value in default_state.items():
        if key.startswith('_'):
            continue
        value = raw.get(key)
        if isinstance(value, type(default_value)):
            state[key] = value

    state['_version'] = raw.get('_version', STORAGE_VERSION)
    return state


def load_json_storage(path: str, default_state: dict[str, Any]) -> dict[str, Any]:
    storage_path = Path(path)
    storage_path.parent.mkdir(parents=True, exist_ok=True)

    if not storage_path.exists() or storage_path.stat().st_size == 0:
        state = deepcopy(default_state)
        state['_version'] = STORAGE_VERSION
        save_json_storage(str(storage_path), state)
        return state

    try:
        with storage_path.open('r', encoding='utf-8') as file:
            raw = json.load(file)
    except json.JSONDecodeError:
        broken_path = storage_path.with_suffix(storage_path.suffix + '.broken')
        os.replace(storage_path, broken_path)

        state = deepcopy(default_state)
        state['_version'] = STORAGE_VERSION
        save_json_storage(str(storage_path), state)
        return state

    if not isinstance(raw, dict):
        broken_path = storage_path.with_suffix(storage_path.suffix + '.broken')
        os.replace(storage_path, broken_path)

        state = deepcopy(default_state)
        state['_version'] = STORAGE_VERSION
        save_json_storage(str(storage_path), state)
        return state

    return _normalize_state(raw, default_state)


def save_json_storage(path: str, state: dict[str, Any]) -> None:
    storage_path = Path(path)
    storage_path.parent.mkdir(parents=True, exist_ok=True)

    data = deepcopy(state)
    data['_version'] = STORAGE_VERSION

    temp_path = storage_path.with_suffix(storage_path.suffix + '.tmp')

    with temp_path.open('w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

    os.replace(temp_path, storage_path)
