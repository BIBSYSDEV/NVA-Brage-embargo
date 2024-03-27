#!/bin/bash

instance=$1

cp /brageexports/$instance/FileEmbargo.txt /brageexports/embargo_sjekk/files/.

input_file="/brageexports/embargo_sjekk/files/FileEmbargo.txt"

# Phrases to filter out
phrases=("pdf.txt" "pdf.jpg" "license_rdf" "license.txt")

# Temporary file for intermediate processing
temp_file="temp.csv"

# Filtering lines based on phrases and writing to temporary file
for phrase in "${phrases[@]}"; do
    grep -v "$phrase" "$input_file" > "$temp_file"
    mv "$temp_file" "$input_file"
done
