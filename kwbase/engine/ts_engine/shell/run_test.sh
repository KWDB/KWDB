#!/bin/bash
set -e

test_file=${2-'*'}
for file in `find $1/tests -name "*$test_file*" -type f`
do
    $file
done

