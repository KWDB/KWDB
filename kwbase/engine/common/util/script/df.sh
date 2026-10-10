#!/bin/bash

squashfs_info=false

if [ "$#" -eq 1 ]; then
    if [ "$1" == "--squashfs" ]; then
        squashfs_info=true
    elif [ "$1" == "--help" ]; then
        echo "Usage: $0 [--squashfs]"
        exit 0
    else
        echo "Invalid parameter: $1"
        echo "Usage: $0 [--squashfs]"
        exit 1
    fi
elif [ "$#" -gt 1 ]; then
    echo "Invalid parameter: $1"
    echo "Usage: $0 [--squashfs]"
    exit 1
fi

# Get the number of mounted squashfs file systems
squashfs_count=$(df -T | grep squashfs | wc -l)

if [ "$squashfs_info" == true ]; then
    # Only the df -h information of the squashfs file system is displayed
    df -hT | grep -E "^Filesystem|squashfs"
else
    # The output does not contain df -h information about the squashfs file system
    echo "File systems information(excluding squashfs):"
    df -hT | grep -v squashfs
    echo
    echo "Number of squashfs mounted: $squashfs_count"
fi

