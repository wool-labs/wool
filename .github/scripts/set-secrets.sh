#!/bin/bash

USAGE="Usage: $0 [-k|--keychain <keychain-name>] [-l|--logout]"

if [[ "$1" == "--help" ]]; then
    echo $USAGE
    exit 0
fi

LOGOUT=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -l|--logout) LOGOUT=true;;
        -k|--keychain) KEYCHAIN="$2"; shift ;;
        *) echo $USAGE; exit 1 ;;
    esac
    shift
done

if [[ ! $(gh auth status) ]]; then
    gh auth login
fi
for KEY in "pypi-token" "wool-labs-app-id" "wool-labs-installation-id" "wool-labs-private-key"; do
    if [[ -n "$KEYCHAIN" && -n $(ks -k $KEYCHAIN ls | grep "\b$KEY\b") ]]; then
        echo "Using $KEY from keychain"
        SECRET=$(ks -k $KEYCHAIN show $KEY)
    else
        read -sp "Enter a value for $KEY: " SECRET
        if [[ -z "$SECRET" ]]; then
            echo ""
            echo "Error: A value for $KEY is required."
            exit 1
        fi
    fi
    gh secret set $(echo $KEY | tr '-' '_' | tr '[:lower:]' '[:upper:]') --app actions --body $SECRET
done
if [[ "$LOGOUT" == true ]]; then
    gh auth logout
fi
