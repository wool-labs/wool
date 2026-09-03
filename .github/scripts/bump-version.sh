#!/bin/bash

USAGE="Usage: $0 major|minor|patch VERSION"

# Evaluate arguments
case $# in
    2)
        ;;
    *)
        echo $USAGE
        exit 1
        ;;
esac

# Evaluate version segment
case $1 in
    major|minor|patch)
        SEGMENT=$1
        ;;
    *)
        echo "ERROR: Invalid version segment: $1" >&2
        echo $USAGE
        exit 1
        ;;
esac

# Evaluate version
VERSION=$2
if ! [[ "$VERSION" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+(-(a|b|rc)[0-9]+)?$ ]]; then
    # Without this the arithmetic below fails on stderr, echoes the version
    # back unchanged and still exits 0, which publishes whatever it was given.
    echo "ERROR: Invalid version: $VERSION" >&2
    echo "$USAGE" >&2
    exit 1
fi

# Determine release cycle
case $VERSION in
    *-a*)
        CYCLE="-a"
        PRE_RELEASE=true
        ;;
    *-b*)
        CYCLE="-b"
        PRE_RELEASE=true
        ;;
    *-rc*)
        CYCLE="-rc"
        PRE_RELEASE=true
        ;;
    *)
        CYCLE=""
        PRE_RELEASE=false
        ;;
esac

if [ "$PRE_RELEASE" = true ] && [[ "$SEGMENT" == "major" ]]; then
    echo "ERROR: Cannot bump major version segment of a pre-release version" >&2
    exit 1
fi

#Split version
read MAJOR MINOR PATCH <<< $("$(dirname "${BASH_SOURCE[0]}")/split-version.sh" $VERSION)

case $SEGMENT in
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        ;;
    minor)
        case $CYCLE in
            "")
                MINOR=$((MINOR + 1))
                ;;
            "-a")
                MINOR=$((MINOR))
                CYCLE="-b"
            ;;
            "-b")
                MINOR=$((MINOR))
                CYCLE="-rc"
            ;;
            "-rc")
                MINOR=$((MINOR))
                CYCLE=""
            ;;
        esac
        PATCH=0
        ;;
    patch)
        if [ -z "$PATCH" ]; then
            PATCH=0
        fi
        PATCH=$((PATCH + 1))
        ;;
esac

if [[ -n "$CYCLE" ]]; then
    echo "v$MAJOR.$MINOR.0$CYCLE$PATCH"
else
    echo "v$MAJOR.$MINOR.$PATCH"
fi
