#!/bin/bash

USAGE="Usage: $0 major|minor [BRANCH=release]"

# Evaluate arguments
case $1 in
    major|minor)
        RELEASE_TYPE=$1
        ;;
    *)
        echo "ERROR: Invalid release type: $1" >&2
        echo "$USAGE" >&2
        exit 1
        ;;
esac
case $# in
    1)
        BRANCH="release"
        ;;
    2)
        BRANCH=$2
        ;;
    *)
        echo "$USAGE" >&2
        exit 1
        ;;
esac

git fetch --unshallow >/dev/null 2>&1

# The release is cut from main, so failing to get there must stop the cut --
# discarding the exit status here would cut and push a release branch from
# whatever HEAD happened to be on.
if ! git checkout main >/dev/null 2>&1; then
    echo "ERROR: Cannot check out 'main'." >&2
    exit 1
fi
if ! git pull >/dev/null 2>&1; then
    echo "ERROR: Cannot update 'main' from origin." >&2
    exit 1
fi

# Check if the release branch already exists, locally or on the remote. A
# CI checkout has only the branch it cloned, so an existing release branch
# is visible there as a remote ref and nowhere else.
if git show-ref --verify --quiet "refs/heads/$BRANCH" ||
    git ls-remote --exit-code --heads origin "$BRANCH" >/dev/null 2>&1; then
    echo "ERROR: Branch '$BRANCH' already exists." >&2
    exit 1
fi

# Get the latest version tag reachable from main, default to 0.0.0
VERSION=$("$(dirname "${BASH_SOURCE[0]}")/latest-version.sh" any)

# Verify no active release candidates exist
if [[ $VERSION == *-rc* ]]; then
    echo "ERROR: An active release candidate already exists: $VERSION" >&2
    exit 1
fi

read MAJOR MINOR PATCH <<< $("$(dirname "${BASH_SOURCE[0]}")/split-version.sh" $VERSION)

# Bump the version
case $RELEASE_TYPE in
    major)
        RELEASE_VERSION="$((MAJOR + 1)).0.0-rc0"
        ;;
    minor)
        RELEASE_VERSION="${MAJOR}.$((MINOR + 1)).0-rc0"
        ;;
esac

RELEASE_TAG="v$RELEASE_VERSION"

# Create a new branch for the release candidate
OUTPUT=$(git checkout -b $BRANCH >/dev/null 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to create branch '$BRANCH'." >&2
    echo "$OUTPUT" >&2
    exit 1
fi
OUTPUT=$(git push origin $BRANCH 2>&1)
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to push branch '$BRANCH' to origin." >&2
    echo "$OUTPUT" >&2
    exit 1
fi

echo $RELEASE_TAG
