"""Readable Plan addresses, independent of storage and execution."""
from collections.abc import Mapping
from typing import Any


def invocation_addresses(document: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    boundaries = {row['id']: row for row in document.get('boundaries', [])}
    def ancestors(identifier, seen=()):
        if identifier is None:
            return ()
        if identifier in seen:
            raise ValueError('cyclic Plan boundaries')
        row = boundaries[identifier]
        key = row.get('authored_key')
        if not key:
            raise ValueError(f'boundary {identifier!r} needs a readable authored key; re-author the Plan')
        return ancestors(row.get('parent_id'), (*seen, identifier)) + (key,)
    result = {}
    for row in document.get('invocations', []):
        key = row.get('authored_key')
        if not key:
            raise ValueError(f'invocation {row["id"]!r} needs a readable authored key; re-author the Plan')
        result[row['id']] = ancestors(row.get('boundary_id')) + (key,)
    return result


def display_address(parts) -> str:
    return '/'.join(part.replace('/', '%2F') for part in parts)


def resolve_invocation(addresses, address: str) -> str:
    if address in addresses:
        return address
    parts = tuple(address.split('/'))
    if any('%' in part.replace('%2F', '') for part in parts):
        raise KeyError('only %2F is supported as an invocation address escape')
    decoded = tuple(part.replace('%2F', '/') for part in parts)
    exact = [key for key, value in addresses.items() if tuple(value) == decoded]
    if exact:
        return exact[0]
    matches = [key for key, value in addresses.items() if len(decoded) == 1 and value[-1] == decoded[0]]
    if len(matches) == 1:
        return matches[0]
    if matches:
        raise KeyError(f'ambiguous invocation {address!r}; candidates: ' + ', '.join(display_address(addresses[key]) for key in matches))
    raise KeyError(f'no invocation at {address!r}')
