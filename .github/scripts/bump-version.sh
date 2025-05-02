#!/bin/bash

USAGE="Usage: $0 major|minor|patch VERSION [short|full=short]"

# Evaluate arguments
case $# in
    3)
        case $3 in
            short)
                FULL=false
                ;;
            full)
                FULL=true
                ;;
            *)
                FULL=false
                ;;
        esac
        ;;
    2)
        FULL=false
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

# Determine release cycle
VERSION=$2
case $VERSION in
    *a*)
        CYCLE="a"
        PRE_RELEASE=true
        ;;
    *b*)
        CYCLE="b"
        PRE_RELEASE=true
        ;;
    *rc*)
        CYCLE="rc"
        PRE_RELEASE=true
        ;;
    *)
        CYCLE="."
        PRE_RELEASE=false
        ;;
esac

if [ "$PRE_RELEASE" = true ] && [[ "$SEGMENT" == "major" ]]; then
    echo "ERROR: Cannot bump major version segment of a pre-release version" >&2
    exit 1
fi

#Split version
read MAJOR MINOR PATCH <<< $(.github/scripts/split-version.sh $VERSION)

case $SEGMENT in
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        ;;
    minor)
        case $CYCLE in
            ".")
                MINOR=$((MINOR + 1))
                ;;
            "a")
                MINOR=$((MINOR))
                CYCLE="b"
            ;;
            "b")
                MINOR=$((MINOR))
                CYCLE="rc"
            ;;
            "rc")
                MINOR=$((MINOR))
                CYCLE="."
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

if [ "$CYCLE" == "." ] && [ "$PATCH" -eq 0 ] && [ "$FULL" == false ]; then
    echo "v$MAJOR.$MINOR"
else
    echo "v$MAJOR.$MINOR$CYCLE$PATCH"
fi
