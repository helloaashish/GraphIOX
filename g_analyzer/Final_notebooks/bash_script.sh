#!/bin/bash
# filepath: /usr/workspace/kogiou1/software/dftracer/examples/dfanalyzer/create_datasets_csv.sh

# Check if input file is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <input_file_or_pattern>"
    echo "Example: $0 /path/to/trace.json"
    echo "Example: $0 '/path/to/*.gz' (for compressed files)"
    exit 1
fi

INPUT="$1"
OUTPUT_CSV="Q1_datasets.csv"

# Function to process files (handles both regular and compressed files)
process_files() {
    if [[ "$INPUT" == *.gz ]]; then
        if [[ "$INPUT" == *"*"* ]]; then
            zcat $INPUT 2>/dev/null
        else
            zcat "$INPUT" 2>/dev/null
        fi
    else
        if [[ "$INPUT" == *"*"* ]]; then
            cat $INPUT 2>/dev/null
        else
            cat "$INPUT" 2>/dev/null
        fi
    fi
}

echo "Extracting FH events and creating datasets..."

# Step 1: Extract FH events and create temporary file with file info
FH_TEMP="fh_events.tmp"
process_files | \
    grep '"name":"FH"' | \
    jq -r '.args | "\(.name)|\(.value)"' > "$FH_TEMP"

# Step 2: Create detailed FH events mapping with dataset, value, pid, tid
FH_DETAILED="fh_events_detailed.tmp"
process_files | \
    grep '"name":"FH"' | \
    jq -r '"\(.args.name)|\(.args.value)|\(.pid)|\(.tid)"' > "$FH_DETAILED.raw"

# Step 3: Create dataset mapping with generic names and link to FH details
DATASET_MAP="dataset_map.tmp"

# Debug: Show sample filenames being processed
echo "Sample filenames being processed:"
head -5 "$FH_TEMP" | cut -d'|' -f1
echo "..."

cat "$FH_TEMP" | awk -F'|' '{
    filename = $1
    filehash = $2
    
    # Debug: Print original filename for first few entries
    if (NR <= 3) print "Original filename: " filename
    
    # Extract extension
    if (match(filename, /\.([^.]+)$/, ext)) {
        extension = toupper(ext[1])
    } else {
        extension = "UNKNOWN"
    }
    
    # Create generic name by removing numbers and specific patterns
    generic_name = filename
    
    # Remove leading ./
    gsub(/^\.\//, "", generic_name)
    
    # Special handling for NA and HG samples FIRST - replace in the original filename
    # Match NA/HG samples anywhere in the path
    gsub(/NA[0-9]+/, "NA", generic_name)
    gsub(/HG[0-9]+/, "HG", generic_name)
    
    # Debug: Print transformed names for first few NA entries
    if (generic_name ~ /NA/ && NR <= 10) {
        print "Original filename: " filename > "/dev/stderr"
        print "After NA replacement: " generic_name > "/dev/stderr"
        print "---" > "/dev/stderr"
    }
    
    # Remove specific numbers (chr10 -> chr, mutations10 -> mutations, etc.)
    gsub(/chr[0-9]+/, "chr", generic_name)
    gsub(/mutations[0-9]+/, "mutations", generic_name)
    gsub(/[0-9]+_/, "", generic_name)
    gsub(/_[0-9]+/, "", generic_name)
    gsub(/[0-9]+\./, ".", generic_name)
    
    # Remove file-specific suffixes (like SAS806, specific IDs)
    gsub(/_[A-Z]+[0-9]+/, "", generic_name)
    gsub(/[A-Z]+[0-9]+/, "", generic_name)
    
    # Extract directory structure for grouping
    if (match(generic_name, /^([^\/]+\/[^\/]+)/, dir)) {
        base_path = dir[1]
    } else if (match(generic_name, /^([^\/]+)/, dir)) {
        base_path = dir[1]
    } else {
        base_path = "misc"
    }
    
    # Create dataset name
    dataset_name = extension " " base_path
    
    # Debug: Print transformed names for first few entries
    if (NR <= 3) {
        print "Generic name: " generic_name
        print "Dataset name: " dataset_name
        print "---"
    }
    
    print dataset_name "|" filehash "|" filename
}' | sort | uniq > "$DATASET_MAP"

# Step 4: Create final FH events file with dataset mapping
# Format: dataset_name|value|pid|tid|filename
awk -F'|' 'NR==FNR {dataset[$2] = $1; next} 
           {split($0, a, "|"); 
            if (dataset[a[2]]) 
                print dataset[a[2]] "|" a[2] "|" a[3] "|" a[4] "|" a[1]
           }' "$DATASET_MAP" "$FH_DETAILED.raw" > "$FH_DETAILED"

echo "Created detailed FH events mapping with $(wc -l < "$FH_DETAILED") entries"

# Debug: Show sample dataset names created
echo "Sample dataset names created:"
head -10 "$DATASET_MAP" | cut -d'|' -f1
echo "..."

# Step 5: Count files per dataset and select sample file for size estimation
DATASET_SUMMARY="dataset_summary.tmp"
cat "$DATASET_MAP" | awk -F'|' '{
    dataset = $1
    filehash = $2
    filename = $3
    
    dataset_count[dataset]++
    if (!dataset_sample[dataset]) {
        dataset_sample[dataset] = filehash
        dataset_sample_name[dataset] = filename
    }
}
END {
    for (dataset in dataset_count) {
        print dataset "|" dataset_count[dataset] "|" dataset_sample[dataset] "|" dataset_sample_name[dataset]
    }
}' | sort -t'|' -k2 -nr > "$DATASET_SUMMARY"

# Step 6: If more than 30 datasets, group them further
DATASET_COUNT=$(wc -l < "$DATASET_SUMMARY")
if [ "$DATASET_COUNT" -gt 30 ]; then
    echo "Found $DATASET_COUNT datasets, grouping further to stay under 30..."
    
    # Group by extension and first part of path only, plus special grouping for sample IDs
    cat "$DATASET_SUMMARY" | awk -F'|' '{
        dataset = $1
        count = $2
        sample_hash = $3
        sample_name = $4
        
        # Special grouping for NA and HG sample datasets
        # Look for any dataset containing NA or HG patterns
        if (dataset ~ /NA/ && dataset ~ /chrn\/chr\./) {
            # Extract extension (e.g., "TXT", "VCF", etc.)
            if (match(dataset, /^([A-Z]+) /, ext_match)) {
                ext = ext_match[1]
            } else {
                ext = "TXT"  # default
            }
            simplified = ext " NA chrn/chr."
        }
        else if (dataset ~ /HG/ && dataset ~ /chrn\/chr\./) {
            # Extract extension
            if (match(dataset, /^([A-Z]+) /, ext_match)) {
                ext = ext_match[1]
            } else {
                ext = "TXT"  # default
            }
            simplified = ext " HG chrn/chr."
        }
        # Keep original names for other datasets
        else {
            simplified = dataset
        }
        
        grouped_count[simplified] += count
        grouped_size[simplified] += count * 0.5  # Rough size estimate for grouping
        if (!grouped_sample[simplified]) {
            grouped_sample[simplified] = sample_hash
            grouped_sample_name[simplified] = sample_name
        }
    }
    END {
        for (dataset in grouped_count) {
            print dataset "|" grouped_count[dataset] "|" grouped_sample[dataset] "|" grouped_sample_name[dataset]
        }
    }' | sort -t'|' -k2 -nr > "$DATASET_SUMMARY.grouped"
    
    mv "$DATASET_SUMMARY.grouped" "$DATASET_SUMMARY"
    
    # Check if still over 30, group even more aggressively
    NEW_COUNT=$(wc -l < "$DATASET_SUMMARY")
    if [ "$NEW_COUNT" -gt 30 ]; then
        echo "Still $NEW_COUNT datasets, grouping more aggressively..."
        cat "$DATASET_SUMMARY" | awk -F'|' '{
            dataset = $1
            count = $2
            sample_hash = $3
            sample_name = $4
            
            # Keep original dataset names, just group by extension if absolutely necessary
            if (match(dataset, /^([A-Z]+)/, ext)) {
                simplified = dataset  # Keep original name first
            } else {
                simplified = "MISC files"
            }
            
            grouped_count[simplified] += count
            if (!grouped_sample[simplified]) {
                grouped_sample[simplified] = sample_hash
                grouped_sample_name[simplified] = sample_name
            }
        }
        END {
            for (dataset in grouped_count) {
                print dataset "|" grouped_count[dataset] "|" grouped_sample[dataset] "|" grouped_sample_name[dataset]
            }
        }' | sort -t'|' -k2 -nr | head -30 > "$DATASET_SUMMARY.final"
        
        mv "$DATASET_SUMMARY.final" "$DATASET_SUMMARY"
    fi
fi

echo "Creating CSV with size estimates..."

# Step 7: Calculate sizes and create final CSV
echo "Dataset_Name,Number_of_Files,Total_Size_MB" > "$OUTPUT_CSV"

while IFS='|' read -r dataset_name file_count sample_hash sample_filename; do
    echo "Processing dataset: $dataset_name ($file_count files, sample hash: $sample_hash)"
    
    # Find all read events for just the sample file hash
    sample_file_bytes=$(process_files | \
        grep -E '"name":"(read|fread)"' | \
        jq -r --arg hash "$sample_hash" '
            select(.args.fhash == $hash) | .args.ret // 0
        ' | \
        awk '{sum += $1} END {print sum + 0}')
    
    echo "Sample file total bytes: $sample_file_bytes"
    
    # Calculate dataset size by multiplying sample file size by number of files
    if [ "$sample_file_bytes" -gt 0 ]; then
        sample_file_size_mb=$(echo "scale=4; $sample_file_bytes / (1024 * 1024)" | bc -l)
        total_dataset_size_mb=$(echo "scale=2; $sample_file_size_mb * $file_count" | bc -l)
        echo "Sample file size: $sample_file_size_mb MB"
        echo "Total dataset size: $total_dataset_size_mb MB ($file_count files)"
        echo "$dataset_name,$file_count,$total_dataset_size_mb" >> "$OUTPUT_CSV"
    else
        echo "No read data found for sample file hash: $sample_hash"
        echo "$dataset_name,$file_count,0" >> "$OUTPUT_CSV"
    fi
    echo "---"
    
done < "$DATASET_SUMMARY"

# Clean up temporary files (keep fh_events_detailed.tmp for next analysis scripts)
rm -f "$FH_TEMP" "$DATASET_MAP" "$DATASET_SUMMARY" "$FH_DETAILED.raw"

echo "Dataset analysis complete! Results saved to $OUTPUT_CSV"
echo "Detailed FH events mapping saved to $FH_DETAILED for use by next analysis scripts"
echo "Number of datasets created: $(( $(wc -l < "$OUTPUT_CSV") - 1 ))"

# Show top 10 datasets by size
echo ""
echo "Top 10 datasets by total size:"
tail -n +2 "$OUTPUT_CSV" | sort -t',' -k3 -nr | head -10 | \
    awk -F',' '{printf "%-50s %8s files %12s MB\n", $1, $2, $3}'

# Email notification
EMAIL="ok22b@fsu.edu"
SUBJECT="Dataset Analysis Complete - $(date)"
BODY="Dataset analysis has completed successfully.

Results:
- Output file: Q1_datasets.csv
- Number of datasets: $(( $(wc -l < "$OUTPUT_CSV") - 1 ))
- Working directory: $(pwd)
- Input: $INPUT

Top 5 datasets by size:
$(tail -n +2 "$OUTPUT_CSV" | sort -t',' -k3 -nr | head -5 | awk -F',' '{printf "%-40s %8s files %12s MB\n", $1, $2, $3}')

Summary of dataset types:
$(tail -n +2 "$OUTPUT_CSV" | awk -F',' '{
    gsub(/[0-9]+ /, "", $1); 
    type_count[$1]++; 
    type_files[$1] += $2; 
    type_size[$1] += $3
} 
END {
    for (type in type_count) {
        printf "- %s: %d datasets, %d files, %.2f MB\n", type, type_count[type], type_files[type], type_size[type]
    }
}')
"

echo "$BODY" | mail -s "$SUBJECT" "$EMAIL"
echo "Email notification sent to $EMAIL"