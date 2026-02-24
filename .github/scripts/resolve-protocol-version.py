"""Resolve the wool-protocol version status for CI gating.

Parses wool/pyproject.toml to find the wool-protocol dependency,
then queries PyPI to determine whether a stable release exists
for the pinned version.

Outputs (via $GITHUB_OUTPUT or stdout):
    stable      — 'true' if a matching stable release exists
    prerelease  — highest matching pre-release version, or 'none'
    is_latest   — 'true' if no stable release with a higher
                  (major, minor) tuple exists
"""

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

import tomllib
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import Version


def _find_protocol_requirement(pyproject: dict) -> Requirement | None:
    """Return the wool-protocol :py:class:`Requirement`, if present."""
    for dep in pyproject.get("project", {}).get("dependencies", []):
        req = Requirement(dep)
        if req.name == "wool-protocol":
            return req
    return None


def _extract_base_version(specifier: SpecifierSet) -> tuple[int, int]:
    """Extract ``(major, minor)`` from a specifier like ``~=0.1``."""
    for spec in specifier:
        v = Version(spec.version)
        return (v.major, v.minor)
    raise ValueError(f"Cannot extract base version from {specifier}")


def _fetch_pypi_versions(package: str) -> list[Version] | None:
    """Fetch all released versions from PyPI. Return *None* on 404."""
    url = f"https://pypi.org/pypi/{package}/json"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    return [Version(v) for v in data.get("releases", {}) if data["releases"][v]]


def _write_outputs(outputs: dict[str, str]) -> None:
    """Write outputs to ``$GITHUB_OUTPUT`` or fall back to stdout."""
    gh_output = os.environ.get("GITHUB_OUTPUT")
    if gh_output:
        with open(gh_output, "a") as f:
            for key, value in outputs.items():
                f.write(f"{key}={value}\n")
    else:
        for key, value in outputs.items():
            print(f"{key}={value}")


def main() -> None:
    pyproject_path = Path(__file__).resolve().parents[2] / "wool" / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)

    req = _find_protocol_requirement(pyproject)
    if req is None:
        _write_outputs({"stable": "true", "prerelease": "none", "is_latest": "true"})
        return

    base_major, base_minor = _extract_base_version(req.specifier)

    versions = _fetch_pypi_versions("wool-protocol")
    if versions is None:
        _write_outputs({"stable": "false", "prerelease": "none", "is_latest": "true"})
        return

    matching_stable = [
        v
        for v in versions
        if not v.is_prerelease and v.major == base_major and v.minor == base_minor
    ]

    matching_pre = [
        v
        for v in versions
        if v.is_prerelease and v.major == base_major and v.minor == base_minor
    ]

    stable = len(matching_stable) > 0

    prerelease = str(max(matching_pre)) if matching_pre else "none"

    higher_stable = [
        v
        for v in versions
        if not v.is_prerelease and (v.major, v.minor) > (base_major, base_minor)
    ]
    is_latest = len(higher_stable) == 0

    _write_outputs(
        {
            "stable": str(stable).lower(),
            "prerelease": prerelease,
            "is_latest": str(is_latest).lower(),
        }
    )


if __name__ == "__main__":
    main()
