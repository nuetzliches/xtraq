# .venv/Scripts/python.exe tools/find_unresolved_json.py
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple, cast

BASE_DIR = Path('debug/.xtraq')

ResultEntry = Tuple[str, str]
Trail = List[str]

results: List[ResultEntry] = []


def visit(node: Any, trail: Trail, path: str) -> None:
    if isinstance(node, dict):
        node_dict = cast(Dict[str, Any], node)
        name = node_dict.get('Name')
        if name is not None and len(node_dict) == 1:
            if any(segment == 'Columns' for segment in trail) or any(segment == 'Parameters' for segment in trail):
                joined_trail = '/'.join(trail + [str(name)])
                results.append((path, joined_trail))
        for key, value in node_dict.items():
            visit(value, trail + [str(key)], path)
    elif isinstance(node, list):
        node_list = cast(List[Any], node)
        for idx, item in enumerate(node_list):
            visit(item, trail + [str(idx)], path)


if BASE_DIR.exists():
    for file in BASE_DIR.rglob('*.json'):
        if file.name == 'index.json':
            continue
        try:
            with file.open('r', encoding='utf-8') as handle:
                data = json.load(handle)
        except Exception:
            continue
        visit(data, [], str(file).replace('\\', '/'))

for path, location in results:
    print(f"{path}: {location}")

print(f"Total: {len(results)}")
