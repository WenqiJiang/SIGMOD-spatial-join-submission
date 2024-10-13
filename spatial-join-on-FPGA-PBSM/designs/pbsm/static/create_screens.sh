#!/bin/bash

echo "Script started."

folders=(
    "1/"
    "2/"
    "4/"
    "8/"
    "16/"
)

for folder_name in "${folders[@]}"; do
    # generate a screen name
    screen_name="screen_${folder_name//\//_}"

    echo "Creating screen session: $screen_name"
    echo "Navigating to directory: $folder_name"

    # check if dir exists
    if [ -d "$folder_name" ]; then
        # new screen
        screen -S "$screen_name" -dm bash -c "cd $folder_name && make cleanall && make all TARGET=hw && exec bash"
    else
        echo "Directory $folder_name does not exist. Skip."
    fi
done

echo "Script execution completed."
