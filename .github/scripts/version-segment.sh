#!/bin/bash

USAGE="Usage: $0 BASE_REF HEAD_REF"

# Evaluate arguments
case $# in
    2)
        BASE_REF=$1
        HEAD_REF=$2
        ;;
    *)
        echo "$USAGE" >&2
        exit 1
        ;;
esac

# The branch pair fixes both which segment moves and which release channel
# the version it moves is read from. `master` is the production line, so a
# fix merged into it patches the last production release; `release` carries
# the pending candidate, so merges into it advance that candidate; and
# finalizing `release` into `master` promotes the candidate by consuming its
# release cycle.
case $BASE_REF in
    master)
        case $HEAD_REF in
            release)
                SEGMENT="minor"
                CHANNEL="candidate"
                ;;
            *)
                SEGMENT="patch"
                CHANNEL="production"
                ;;
        esac
        ;;
    release)
        SEGMENT="patch"
        CHANNEL="candidate"
        ;;
    *)
        echo "ERROR: Unsupported base branch $BASE_REF" >&2
        exit 1
        ;;
esac

echo "segment=$SEGMENT"
echo "channel=$CHANNEL"
