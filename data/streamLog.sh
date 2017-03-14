#!/bin/bash

input="access_log.txt"
output_dir="stream_data"
count=0
rm -r $output_dir
mkdir $output_dir

while IFS= read line
do
	if (( $count % 5 == 0 ))
	then
	sleep 2
	output="stream_data/continuous_log_$count.txt"
	touch $output
	fi
	echo "$line" >> $output
	count=$((count+1))
done <"$input"
