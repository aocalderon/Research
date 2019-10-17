#!/bin/bash

input_path=$1
output_path=$2

for entry in "$input_path"/*.tsv
do
    echo "spark-submit --class RemoveDuplicates /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input $entry --output ${output_path}$(basename "$entry")"
    spark-submit --class RemoveDuplicates /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input $entry --output ${output_path}$(basename "$entry")
done
       
