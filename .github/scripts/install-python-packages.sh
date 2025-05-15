#!/bin/bash

# Install all Python namespace packages in the current directory
for dir in */; do
    if [ -d "$dir" ] && [ -f "$dir/pyproject.toml" ]; then
        (cd "$dir" && uv pip install --no-deps .)
    fi
done
