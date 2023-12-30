#!/bin/bash

# Script for generating a file in empty Hydrus directories
# so that git will save them and not cause Hydrus to panic at launch.

# Get the directory where the script is located
script_dir="$(cd "$(dirname "$0")" && pwd)"

# Directory to start from
start_dir="$script_dir/db"

# Name of the file you want to create
file_to_add=".gitkeep"

# Find all subdirectories and create the file if the directory is empty
find "$start_dir" -type d | while read dir; do
    if [ -z "$(ls -A "$dir")" ]; then
        touch "$dir/$file_to_add"
    fi
done
