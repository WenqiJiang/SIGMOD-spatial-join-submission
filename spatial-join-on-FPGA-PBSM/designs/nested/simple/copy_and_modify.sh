#!/bin/bash

# source and dest
src_dir="./1"
dest_dirs=("2" "4" "8" "16")

# line to change
original_line="#define N_JOIN_UNITS 1"

# handle errors
error_exit() {
    echo "$1" 1>&2
    exit 1
}

# loop through dest
for dest_dir in "${dest_dirs[@]}"; do
    # create the dest directory if it doesn't exist
    if ! mkdir -p "$dest_dir" 2>/dev/null; then
        error_exit "Error: cannot create directory $dest_dir."
    fi

    # clean the dest directory if it exists and is writable
    if [ -d "$dest_dir" ]; then
        rm -rf "$dest_dir"/* || error_exit "Error: cannot clean directory $dest_dir."
    fi

    # copy to dest
    cp -r "$src_dir/"* "$dest_dir/" || error_exit "Error: cannot copy files to $dest_dir."

    # get new value
    new_value="$dest_dir"

    # ensure the src directory exists within the dest
    mkdir -p "$dest_dir/src" || error_exit "Error: cannot create src directory in $dest_dir."

    # replace the line in constants.hpp
    sed -i '' "s|$original_line|#define N_JOIN_UNITS $new_value|" "$dest_dir/src/constants.hpp" || error_exit "Error: Cannot modify constants.hpp in $dest_dir/src."
done

# chmod +x copy_and_modify.sh