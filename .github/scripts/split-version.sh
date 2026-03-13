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

# Extract pre-release number if present (-rc, -b, -a)
if [[ $VERSION == *-rc* ]]; then
    PRE=${VERSION##*-rc}
    BASE=${VERSION%-rc*}
elif [[ $VERSION == *-b* ]]; then
    PRE=${VERSION##*-b}
    BASE=${VERSION%-b*}
elif [[ $VERSION == *-a* ]]; then
    PRE=${VERSION##*-a}
    BASE=${VERSION%-a*}
else
    PRE=""
    BASE=$VERSION
fi

# Split the base version into its components
IFS='.' read -r MAJOR MINOR PATCH <<< "$BASE"

if [[ -n "$PRE" ]]; then
    echo "$MAJOR $MINOR $PRE"
else
    echo "$MAJOR $MINOR ${PATCH:-0}"
fi
