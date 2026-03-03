#!/bin/bash

output="all_code_dump.txt"

# Clear file if it exists
> "$output"

# Find .rs and .toml files inside src and tests
find src tests -type f \( -name "*.rs" -o -name "*.toml" \) 2>/dev/null | sort | while read file
do
    echo "==================================================" >> "$output"
    echo "FILE: $file" >> "$output"
    echo "==================================================" >> "$output"
    echo "" >> "$output"

    cat "$file" >> "$output"

    echo -e "\n\n" >> "$output"
done

echo "✅ Code dumped into $output"