import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import _git as _git
import _version
from hatchling.metadata.plugin.interface import MetadataHookInterface


class WoolMetadataHook(MetadataHookInterface):
    PLUGIN_NAME = "wool-metadata"

    def update(self, metadata):
        version = _version.SemanticVersion.parse.git()
        metadata["version"] = str(version)
