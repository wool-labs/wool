#!/bin/bash

if [ -d "$(pwd)/..git" ]; then
    echo "$(pwd)"
elif [ -d "$(pwd)/.github" ]; then
    echo "$(pwd)/.github"
else
    echo "."
fi
