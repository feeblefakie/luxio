#!/bin/sh

usage() {
    echo "$0 num_records"
    exit 1
}

if [ $# -ne 1 ]; then
    usage
fi

file=random_data.$1
./get_random_data $1 > $file
sort -n $file > $file.sorted
