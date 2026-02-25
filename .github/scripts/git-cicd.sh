#!/bin/bash

if [ -d "$(pwd)/..git" ]; then
    GIT_DIR="$(pwd)/..git"
    WORK_TREE="$(pwd)"
elif [ -d "$(pwd)/.github" ]; then
    GIT_DIR="$(pwd)/.github/..git"
    WORK_TREE="$(pwd)/.github"
else
    echo "Neither '.git' nor '.github' directory found."
    exit 1
fi
# Execute the git command with all the provided arguments
git --git-dir="$GIT_DIR" --work-tree="$WORK_TREE" "$@"
