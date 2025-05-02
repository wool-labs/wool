#!/bin/bash

# Check if Homebrew (brew) is installed
if ! command -v brew &> /dev/null; then

    # Prompt the user to install Homebrew (defaults to "yes")
    read -p "Homebrew (brew) is not installed. Do you want to install it now? (Y/n): " brew_choice
    brew_choice=${brew_choice:-Y}
    if [[ "$brew_choice" == "y" || "$brew_choice" == "Y" ]]; then

        # Install Homebrew using the official installation script
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

        # Confirm the installation
        if ! command -v brew &> /dev/null; then
            echo "Error: Homebrew installation failed."
            exit 1
        fi
    else
        echo "Homebrew is required. Please install it manually."
        exit 1
    fi
else
    echo "Homebrew is already installed."
fi

# Check if UV is installed
if ! command -v uv &> /dev/null; then

    # Prompt the user to install UV (defaults to "yes")
    read -p "UV is required but not installed. Do you want to install it now? (Y/n): " gh_choice
    uv_choice=${uv_choice:-Y}
    if [[ "$uv_choice" == "y" || "$uv_choice" == "Y" ]]; then
        
        # Install UV using Homebrew
        brew install uv

        # Confirm the installation
        if ! command -v uv &> /dev/null; then
            echo "Error: UV installation failed."
            exit 1
        fi
    else
        echo "UV is required. Please install it manually."
        exit 1
    fi
else    
    echo "UV is already installed."
fi

# Check if the GitHub CLI is installed
if ! command -v gh &> /dev/null; then

    # Prompt the user to install GitHub CLI (defaults to "yes")
    read -p "GitHub CLI (gh) is required but not installed. Do you want to install it now? (Y/n): " gh_choice
    gh_choice=${gh_choice:-Y}
    if [[ "$gh_choice" == "y" || "$gh_choice" == "Y" ]]; then
        
        # Install GitHub CLI using Homebrew
        brew install gh

        # Confirm the installation
        if ! command -v gh &> /dev/null; then
            echo "Error: GitHub CLI installation failed."
            exit 1
        fi
    else
        echo "GitHub CLI is required. Please install it manually."
        exit 1
    fi
else    
    echo "GitHub CLI is already installed."
fi

# Check if the keychain secrets manager (ks) is installed
if ! command -v ks &> /dev/null; then

    # Prompt the user to install ks (defaults to "yes")
    read -p "Keychain secrets manager is required but not installed. Do you want to install it now? (Y/n): " ks_choice
    ks_choice=${ks_choice:-Y}
    if [[ "$ks_choice" == "y" || "$ks_choice" == "Y" ]]; then
        
        # Install keychain secrets manager using Homebrew
        brew tap loteoo/formulas
        brew install ks

        # Confirm the installation
        if ! command -v ks &> /dev/null; then
            echo "Error: Keychain secrets manager installation failed."
            exit 1
        fi
    else
        echo "Keychain secrets manager is required. Please install it manually."
        exit 1
    fi
else
    echo "Keychain secrets manager is already installed."
fi
