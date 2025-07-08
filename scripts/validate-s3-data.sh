#!/bin/bash

# validate-s3-data.sh
# Validate Avro data integrity in S3

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Load environment
if [ -f ".env.localstack" ]; then
    source .env.localstack
fi

S3_ENDPOINT_URL=""
if [ ! -z "$S3_ENDPOINT" ]; then
    S3_ENDPOINT_URL="--endpoint-url=$S3_ENDPOINT"
fi

echo -e "${BLUE}🔍 S3 Avro Data Validation${NC}"
echo -e "${BLUE}==========================${NC}"

if [ -z "$S3_BUCKET" ]; then
    echo -e "${RED}❌ S3_BUCKET not set. Please source .env.localstack${NC}"
    exit 1
fi

# Download Avro tools if not present
AVRO_TOOLS="avro-tools-1.11.3.jar"
if [ ! -f "$AVRO_TOOLS" ]; then
    echo "📥 Downloading Avro tools..."
    wget -q https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.11.3/avro-tools-1.11.3.jar
fi

# Create temp directory
TEMP_DIR="temp-validation-$(date +%s)"
mkdir -p "$TEMP_DIR"
cd "$TEMP_DIR"

echo "📋 Bucket: $S3_BUCKET"
echo "🗂️  Working directory: $(pwd)"
echo ""

# Function to validate Avro file
validate_avro_file() {
    local file="$1"
    local table="$2"
    
    echo -e "${BLUE}🔍 Validating: $file${NC}"
    
    # Check if file is valid Avro
    if ! java -jar "../$AVRO_TOOLS" getschema "$file" > /dev/null 2>&1; then
        echo -e "${RED}❌ Invalid Avro file: $file${NC}"
        return 1
    fi
    
    # Extract schema
    java -jar "../$AVRO_TOOLS" getschema "$file" > "${file}.schema"
    
    # Convert to JSON for analysis
    java -jar "../$AVRO_TOOLS" tojsonpretty "$file" > "${file}.json"
    
    # Count records
    local record_count=$(cat "${file}.json" | jq -s length)
    echo "   📊 Records: $record_count"
    
    # Validate required fields
    local required_fields=("table_name" "operation" "timestamp" "source")
    local missing_fields=0
    
    for field in "${required_fields[@]}"; do
        if ! jq -e ".[0].$field" "${file}.json" > /dev/null 2>&1; then
            echo -e "${RED}   ❌ Missing field: $field${NC}"
            missing_fields=$((missing_fields + 1))
        else
            echo -e "${GREEN}   ✅ Field present: $field${NC}"
        fi
    done
    
    # Check table name consistency
    local table_names=$(jq -r '.[].table_name' "${file}.json" | sort -u)
    local unique_tables=$(echo "$table_names" | wc -l | tr -d ' ')
    
    if [ "$unique_tables" -eq 1 ] && [ "$table_names" = "$table" ]; then
        echo -e "${GREEN}   ✅ Table name consistent: $table${NC}"
    else
        echo -e "${YELLOW}   ⚠️  Table name inconsistency. Expected: $table, Found: $table_names${NC}"
    fi
    
    # Check operation types
    local operations=$(jq -r '.[].operation' "${file}.json" | sort -u | tr '\n' ',' | sed 's/,$//')
    echo "   🔄 Operations: $operations"
    
    # Check timestamp validity
    local invalid_timestamps=$(jq -r '.[].timestamp | select(. == null or . == 0)' "${file}.json" | wc -l | tr -d ' ')
    if [ "$invalid_timestamps" -eq 0 ]; then
        echo -e "${GREEN}   ✅ All timestamps valid${NC}"
    else
        echo -e "${YELLOW}   ⚠️  $invalid_timestamps invalid timestamps${NC}"
    fi
    
    # Check processing latency
    local avg_latency=$(jq '[.[].processing_latency_ms | select(. != null)] | add / length' "${file}.json" 2>/dev/null || echo "N/A")
    echo "   ⏱️  Average latency: ${avg_latency}ms"
    
    echo ""
    return $missing_fields
}

# Download and validate files
echo "📥 Downloading recent Avro files..."

total_files=0
valid_files=0
invalid_files=0

# Check each table
for table in users orders; do
    echo -e "${BLUE}🔍 Checking table: $table${NC}"
    
    # Download recent files for this table
    aws $S3_ENDPOINT_URL s3 sync s3://$S3_BUCKET/cdc-avro-test/$table/ ./$table/ --exclude "*" --include "*.avro" 2>/dev/null || true
    
    # Find Avro files
    files=$(find ./$table/ -name "*.avro" 2>/dev/null || true)
    
    if [ -z "$files" ]; then
        echo -e "${YELLOW}   ⚠️  No Avro files found for table: $table${NC}"
        continue
    fi
    
    # Validate each file
    while IFS= read -r file; do
        if [ -f "$file" ]; then
            total_files=$((total_files + 1))
            if validate_avro_file "$file" "$table"; then
                valid_files=$((valid_files + 1))
            else
                invalid_files=$((invalid_files + 1))
            fi
        fi
    done <<< "$files"
done

# Summary
cd ..
rm -rf "$TEMP_DIR"

echo -e "${BLUE}📊 Validation Summary${NC}"
echo -e "${BLUE}===================${NC}"
echo "Total files: $total_files"
echo -e "${GREEN}Valid files: $valid_files${NC}"
echo -e "${RED}Invalid files: $invalid_files${NC}"

if [ "$invalid_files" -eq 0 ] && [ "$total_files" -gt 0 ]; then
    echo -e "${GREEN}🎉 All files passed validation!${NC}"
    exit 0
elif [ "$total_files" -eq 0 ]; then
    echo -e "${YELLOW}⚠️  No files found to validate${NC}"
    exit 1
else
    echo -e "${RED}❌ Some files failed validation${NC}"
    exit 1
fi 