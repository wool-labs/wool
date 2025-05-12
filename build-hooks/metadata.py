import os
import sys


sys.path.insert(0, os.path.dirname(__file__))

from copy import copy

import _git as _git
import _version
from hatchling.metadata.plugin.interface import MetadataHookInterface
from packaging.requirements import Requirement


def _get_subpackages(directory):
    subpackages = []
    for entry in os.scandir(directory):
        if entry.is_dir() and entry.name.startswith("wool"):
            subpackages.append(entry.name)
    return subpackages


class WoolMetadataHook(MetadataHookInterface):
    PLUGIN_NAME = "wool-metadata"

    def update(self, metadata):
        subpackages = _get_subpackages(f"{os.path.dirname(__file__)}/..")
        version = _version.PythonicVersion.parse.git()
        metadata["version"] = str(version)
        for dependencies in (
            metadata["dependencies"],
            *metadata["optional-dependencies"].values(),
        ):
            for dependecy in copy(dependencies):
                requirement = Requirement(dependecy)
                if (
                    requirement.name.startswith("wool")
                    and requirement.name in subpackages
                    and not requirement.specifier
                ):
                    if version.local:
                        requirement = f"{requirement.name} @ {{root:parent:uri}}/{requirement.name}"
                    else:
                        requirement.specifier &= f"=={version}"
                    dependencies.remove(dependecy)
                    dependencies.append(str(requirement))
