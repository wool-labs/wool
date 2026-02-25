#!/bin/bash

# Check for the existence of a PR with the label "release" and state "open"
gh pr list --label release --state open --json number --jq '. | length > 0'
