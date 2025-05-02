#!/bin/bash

USAGE="Usage: $0 [-s|--source=dist] [[KEYCHAIN=dev SECRET=pypi-token] | [TOKEN]]"
SOURCE="dist"
KEYCHAIN="dev"
SECRET="pypi-token"

# Parse options
ARGS=()
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -s|--source) 
            # Set source directory
            if [[ -z "$2" ]]; then
                echo "Error: --source requires a value."
                echo $USAGE
                exit 1
            fi
            SOURCE="$2"
            shift
            ;;
        *)
            # Collect arguments
            ARGS+=("$1")
            ;;
    esac
    shift
done
echo "Publishing artifacts in '$SOURCE' directory..."

# Parse arguments
case ${#ARGS[@]} in
    0)
        # Attempt to retrieve token from default keychain
        if command -v ks &> /dev/null; then
            if [[ -n $(ks -k $KEYCHAIN ls | grep "\b$SECRET\b") ]]; then
                echo "Publishing with token from keychain..."
                TOKEN=$(ks -k $KEYCHAIN show $SECRET)
            else
                echo "Warning: Keychain does not contain 'pypi-token' secret."
                echo "Publishing without token..."
            fi
        else
            echo "Warning: Keychain secrets manager (ks) is not installed. Please install it to use keychain secrets."
            echo "Publishing without token..."
        fi
        ;;
    1)
        # Use the specified token
        echo "Publishing with provided token..."
        TOKEN="${ARGS[0]}"
        ;;
    2)
        # Attempt to retrieve token from the specified keychain
        echo "Publishing with token from keychain..."
        KEYCHAIN="${ARGS[0]}"
        SECRET="${ARGS[1]}"
        TOKEN=$(ks -k $KEYCHAIN show $SECRET)
        ;;
    *)
        # Improper usage
        echo $USAGE
        exit 1
        ;;
esac

# Publish the package
if [ -n "$TOKEN" ]; then
    uv publish --username "__token__" --password "$TOKEN" "$SOURCE"/*
else
    uv publish "$SOURCE/*"
fi
