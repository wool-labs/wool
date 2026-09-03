#!/bin/bash

USAGE="Usage: $0 production|candidate|any [REF=HEAD]"

# Evaluate release channel. Each channel matches the whole tag rather than
# excluding cycle markers, so a tag that is not a version this tooling
# produces -- a "nightly-2026-01-01", a "docs-rc-cleanup" -- can never be
# bumped and published as one. A pre-release always carries a zero patch
# segment because that is the only shape bump-version.sh emits, and it is
# the only shape split-version.sh can carry back through a bump.
case $1 in
    production)
        PATTERN='^v[0-9]+\.[0-9]+\.[0-9]+$'
        ;;
    candidate)
        PATTERN='^v[0-9]+\.[0-9]+\.0-rc[0-9]+$'
        ;;
    any)
        PATTERN='^v[0-9]+\.[0-9]+\.([0-9]+|0-(a|b|rc)[0-9]+)$'
        ;;
    *)
        echo "ERROR: Invalid release channel: $1" >&2
        echo "$USAGE" >&2
        exit 1
        ;;
esac

# Evaluate arguments
case $# in
    1)
        REF="HEAD"
        ;;
    2)
        REF=$2
        ;;
    *)
        echo "$USAGE" >&2
        exit 1
        ;;
esac

# An unresolvable ref is an error, not an empty channel. Reporting v0.0.0
# for a ref that does not exist would bump to v0.0.1 and publish it.
if ! git rev-parse --verify --quiet "${REF}^{commit}" >/dev/null 2>&1; then
    echo "ERROR: Cannot resolve ref: $REF" >&2
    echo "$USAGE" >&2
    exit 1
fi

# The highest version of the channel reachable from REF -- deliberately not
# the nearest. `git describe` ranks candidate tags by a commit-distance
# metric that a merge from an older fork point inverts, which would hand
# back a tag below one already released and publish a version regression.
# Tags outside the channel are filtered before the sort, so a commit that
# reaches both a production tag and a candidate resolves by pattern rather
# than by tie-breaking. The suffix settings order a pre-release below the
# version it is a candidate for, which `git`'s version sort otherwise
# inverts.
VERSION=$(
    git -c versionsort.suffix=-a \
        -c versionsort.suffix=-b \
        -c versionsort.suffix=-rc \
        tag --merged "$REF" --sort=-v:refname |
        grep -E "$PATTERN" |
        head -1
)

echo "${VERSION:-v0.0.0}"
