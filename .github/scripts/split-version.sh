#!/bin/bash

USAGE="Usage: $0 [VERSION=0.0.0]"

# Evaluate arguments
case $# in
    1)
        VERSION=$1
        ;;
    *)
        VERSION="0.0.0"
        ;;
esac

# Strip the leading "v" if there is one
if [[ $VERSION == v* ]]; then
    VERSION=${VERSION#v}
fi

# Replace pre-release identifiers with a dot
if [[ $VERSION == *a* ]]; then
    VERSION=${VERSION//a/.}
elif [[ $VERSION == *b* ]]; then
    VERSION=${VERSION//b/.}
elif [[ $VERSION == *rc* ]]; then
    VERSION=${VERSION//rc/.}
fi

# Split the tag into its components
IFS='.' read -r -a VERSION_PARTS <<< "$VERSION"

echo "${VERSION_PARTS[0]} ${VERSION_PARTS[1]} ${VERSION_PARTS[2]}"
