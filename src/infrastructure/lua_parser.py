from __future__ import annotations

from typing import Any, Dict

from luaparser import ast
from luaparser import astnodes


# Support both possible table node classes, depending on luaparser version.
_TABLE_NODE_TYPES = []
if hasattr(astnodes, "TableConstructor"):
    _TABLE_NODE_TYPES.append(astnodes.TableConstructor)
if hasattr(astnodes, "Table"):
    _TABLE_NODE_TYPES.append(astnodes.Table)
TABLE_NODE_TYPES = tuple(_TABLE_NODE_TYPES)


def _expr_to_python(node: astnodes.AST) -> Any:
    """
    Convert a Lua expression node into a Python value.
    """
    if isinstance(node, astnodes.Number):
        return node.n
    if isinstance(node, astnodes.String):
        return node.s

    if isinstance(node, TABLE_NODE_TYPES):
        result: Dict[Any, Any] = {}
        for field in node.fields:
            key_node = field.key
            value_node = field.value

            # Only handle explicit keys for now; array-style fields are ignored.
            if isinstance(key_node, astnodes.Name):
                key = key_node.id
            elif isinstance(key_node, astnodes.String):
                key = key_node.s
            elif isinstance(key_node, astnodes.Number):
                key = key_node.n
            else:
                continue

            result[key] = _expr_to_python(value_node)

        return result

    # Fallback: keep the raw repr so we don't crash on unexpected nodes.
    return repr(node)


def parse_lua_table(text: str) -> Dict[str, Any]:
    """
    Parse Lua wiki modules into a Python dict using luaparser.

    Supports patterns like:

        equipment["Some Name"] = { ... }
        items["Some Name"] = { ... }
        guns["Some Name"] = { ... }

    Returns a mapping: key (string) -> Python dict built from the table body.
    """
    # Parse the full Lua module into an AST.
    tree = ast.parse(text)

    result: Dict[str, Any] = {}

    for node in ast.walk(tree):
        # We care about assignments like: <table_name>["Key"] = { ... }
        if not isinstance(node, astnodes.Assign):
            continue
        if not node.targets or len(node.targets) != 1:
            continue

        target = node.targets[0]
        value = node.values[0] if node.values else None

        if not isinstance(target, astnodes.Index):
            continue
        if value is None or not isinstance(value, TABLE_NODE_TYPES):
            continue

        # target.value should be the table name (e.g. equipment/items/guns),
        # target.idx should be the string key.
        tbl_name_node = target.value
        key_node = target.idx

        if not isinstance(tbl_name_node, astnodes.Name):
            continue
        if not isinstance(key_node, astnodes.String):
            continue

        key = key_node.s
        result[key] = _expr_to_python(value)

    return result


