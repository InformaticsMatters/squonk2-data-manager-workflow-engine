#!/bin/bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
  echo "Usage: $0 <input_file(.smi or .smi.gz)> <lines_per_file> <output_basename> [has_header: yes]"
  exit 1
fi

input_file="$1"
lines_per_file="$2"
base_name="$3"
has_header="${4:-no}"

# Determine how to read the file (plain text or gzipped)
if [[ "$input_file" == *.gz ]]; then
  reader="zcat"
else
  reader="cat"
fi

if ! [[ -f "$input_file" ]]; then
  echo "Error: File '$input_file' not found"
  exit 1
fi

# Extract header if present
if [[ "$has_header" == "yes" ]]; then
  header="$($reader "$input_file" | head -n1)"
  data_start=2
else
  header=""
  data_start=1
fi

# Count number of data lines (excluding header if present)
data_lines="$($reader "$input_file" | tail -n +"$data_start" | wc -l)"
if [[ "$data_lines" -eq 0 ]]; then
  echo "No data lines to process."
  exit 0
fi

# Calculate number of output files and required zero padding
num_files=$(( (data_lines + lines_per_file - 1) / lines_per_file ))
pad_width=0
if [[ "$num_files" -gt 1 ]]; then
  pad_width=${#num_files}
fi

# Split logic
$reader "$input_file" | tail -n +"$data_start" | awk -v header="$header" -v lines="$lines_per_file" -v base="$base_name" -v pad="$pad_width" '
function new_file() {
  suffix = (pad > 0) ? sprintf("%0*d", pad, file_index) : file_index
  file = base "_" suffix ".smi"
  if (header != "") {
    print header > file
  }
  file_index++
  line_count = 0
}
{
  if (line_count == 0) {
    new_file()
  }
  print >> file
  line_count++
  if (line_count == lines) {
    close(file)
    print file " created"
    line_count = 0
  }
}
' file_index=1
