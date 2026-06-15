#!/usr/bin/env python3
"""Emit ``name==floor`` pins for the declared lower bounds in a pyproject.

Given a ``pyproject.toml`` and one or more distribution names, this prints the
exact lower bound (the ``>=`` specifier) declared for each in
``[project].dependencies`` as ``name==<floor>`` tokens, space-separated.

The stub-floor-guard CI job uses this to install the package at its declared
floors and confirm the generated gRPC/protobuf stubs still import there.
Parsing the floors from the metadata (rather than hard-coding them) keeps the
guard honest: it always tests whatever the package currently advertises.
"""

import pathlib
import re
import sys

import tomllib

# A PEP 508 dependency string starts with the distribution name; capture it and
# the lower bound declared by its ``>=`` specifier (if any).
_NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")
_FLOOR = re.compile(r">=\s*([0-9][0-9A-Za-z.+!*-]*)")


def _normalize(name: str) -> str:
    # PEP 503: distribution names are case-insensitive and treat runs of ``-``,
    # ``_``, and ``.`` as equivalent, so normalize both sides before comparing.
    return re.sub(r"[-_.]+", "-", name).lower()


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(
            "usage: dependency-floors.py <pyproject.toml> <name> [<name> ...]",
            file=sys.stderr,
        )
        return 2

    pyproject = pathlib.Path(argv[0])
    names = argv[1:]

    with pyproject.open("rb") as handle:
        data = tomllib.load(handle)
    dependencies = data.get("project", {}).get("dependencies", [])

    floors: dict[str, str] = {}
    for dependency in dependencies:
        name_match = _NAME.match(dependency)
        floor_match = _FLOOR.search(dependency)
        if name_match and floor_match:
            floors[_normalize(name_match.group(1))] = floor_match.group(1)

    pins = []
    for name in names:
        floor = floors.get(_normalize(name))
        if floor is None:
            print(
                f"no '>=' floor declared for {name!r} in {pyproject}",
                file=sys.stderr,
            )
            return 1
        pins.append(f"{name}=={floor}")

    print(" ".join(pins))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
