#!/bin/bash

# monitor-s3-pipeline.sh
# Real-time monitoring for PostgreSQL CDC to S3 pipeline

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Load environment if available
if [ -f ".env.localstack" ]; then
    source .env.localstack
fi

# Configuration
REFRESH_INTERVAL=${REFRESH_INTERVAL:-5}
S3_ENDPOINT_URL=""
if [ ! -z "$S3_ENDPOINT" ]; then
    S3_ENDPOINT_URL="--endpoint-url=$S3_ENDPOINT"
fi

echo -e "${BLUE}🔍 PostgreSQL CDC to S3 Pipeline Monitor${NC}"
echo -e "${BLUE}=========================================${NC}"
echo "Press Ctrl+C to exit"
echo ""

# Function to get timestamp
get_timestamp() {
    date '+%H:%M:%S'
}

# Function to check Flink cluster health
check_flink() {
    local status="❌ OFFLINE"
    local color=$RED
    
    if curl -s http://localhost:8081/config > /dev/null 2>&1; then
        status="✅ ONLINE"
        color=$GREEN
    fi
    
    echo -e "${color}Flink Cluster: $status${NC}"
}

# Function to get Flink job info
get_flink_jobs() {
    if curl -s http://localhost:8081/jobs > /dev/null 2>&1; then
        local jobs=$(curl -s http://localhost:8081/jobs | jq -r '.jobs[]? | "\(.id) \(.name) \(.state)"' 2>/dev/null || echo "")
        
        if [ -z "$jobs" ]; then
            echo -e "${YELLOW}No running jobs${NC}"
        else
            echo "$jobs" | while IFS=' ' read -r id name state; do
                case $state in
                    "RUNNING")
                        echo -e "${GREEN}📊 Job: $name (${state})${NC}"
                        ;;
                    "FAILED"|"CANCELED")
                        echo -e "${RED}💥 Job: $name (${state})${NC}"
                        ;;
                    *)
                        echo -e "${YELLOW}⏳ Job: $name (${state})${NC}"
                        ;;
                esac
            done
        fi
    else
        echo -e "${RED}Cannot connect to Flink${NC}"
    fi
}

# Function to check PostgreSQL
check_postgres() {
    local status="❌ OFFLINE"
    local color=$RED
    
    if pg_isready -h localhost -p 5432 -U cdc_user > /dev/null 2>&1; then
        status="✅ ONLINE"
        color=$GREEN
    fi
    
    echo -e "${color}PostgreSQL: $status${NC}"
}

# Function to check S3 connectivity
check_s3() {
    local status="❌ OFFLINE"
    local color=$RED
    
    if [ -z "$S3_BUCKET" ]; then
        echo -e "${RED}S3: No bucket configured${NC}"
        return
    fi
    
    if aws $S3_ENDPOINT_URL s3 ls s3://$S3_BUCKET/ > /dev/null 2>&1; then
        status="✅ ONLINE"
        color=$GREEN
    fi
    
    echo -e "${color}S3 (${S3_BUCKET}): $status${NC}"
}

# Function to count S3 files
count_s3_files() {
    if [ -z "$S3_BUCKET" ]; then
        return
    fi
    
    local files=$(aws $S3_ENDPOINT_URL s3 ls s3://$S3_BUCKET/ --recursive 2>/dev/null | grep -E "\.(avro|json)$" | wc -l | tr -d ' ')
    local size=$(aws $S3_ENDPOINT_URL s3 ls s3://$S3_BUCKET/ --recursive --summarize 2>/dev/null | grep "Total Size" | awk '{print $3" "$4}' || echo "Unknown")
    
    echo -e "${BLUE}📁 Total Files: $files | Size: $size${NC}"
    
    # Recent files (last 10 minutes)
    local recent_files=$(aws $S3_ENDPOINT_URL s3 ls s3://$S3_BUCKET/ --recursive 2>/dev/null | \
        awk -v cutoff="$(date -d '10 minutes ago' '+%Y-%m-%d %H:%M')" '$1 " " $2 > cutoff' | \
        wc -l | tr -d ' ')
    
    if [ "$recent_files" -gt 0 ]; then
        echo -e "${GREEN}🆕 Recent Files (10min): $recent_files${NC}"
    else
        echo -e "${YELLOW}🆕 Recent Files (10min): 0${NC}"
    fi
}

# Function to show latest S3 files
show_latest_files() {
    if [ -z "$S3_BUCKET" ]; then
        return
    fi
    
    echo -e "${BLUE}📄 Latest S3 Files:${NC}"
    aws $S3_ENDPOINT_URL s3 ls s3://$S3_BUCKET/ --recursive 2>/dev/null | \
        grep -E "\.(avro|json)$" | \
        sort -k1,2 | \
        tail -5 | \
        while read -r line; do
            echo "   $line"
        done
}

# Function to check CDC lag
check_cdc_lag() {
    if ! psql -h localhost -p 5432 -U cdc_user -d cdc_source -t -c "SELECT 1;" > /dev/null 2>&1; then
        return
    fi
    
    # Get latest record timestamps
    local latest_users=$(psql -h localhost -p 5432 -U cdc_user -d cdc_source -t -c "SELECT EXTRACT(EPOCH FROM MAX(created_at)) FROM users;" 2>/dev/null | tr -d ' ' || echo "0")
    local latest_orders=$(psql -h localhost -p 5432 -U cdc_user -d cdc_source -t -c "SELECT EXTRACT(EPOCH FROM MAX(created_at)) FROM orders;" 2>/dev/null | tr -d ' ' || echo "0")
    
    local current_time=$(date +%s)
    local users_lag=$((current_time - ${latest_users%.*}))
    local orders_lag=$((current_time - ${latest_orders%.*}))
    
    echo -e "${BLUE}⏱️  CDC Lag:${NC}"
    echo "   Users: ${users_lag}s ago"
    echo "   Orders: ${orders_lag}s ago"
}

# Main monitoring loop
while true; do
    clear
    echo -e "${BLUE}🔍 PostgreSQL CDC to S3 Pipeline Monitor - $(get_timestamp)${NC}"
    echo -e "${BLUE}=========================================${NC}"
    echo ""
    
    echo -e "${BLUE}🏥 System Health:${NC}"
    check_flink
    check_postgres
    check_s3
    echo ""
    
    echo -e "${BLUE}💼 Flink Jobs:${NC}"
    get_flink_jobs
    echo ""
    
    echo -e "${BLUE}📊 S3 Storage:${NC}"
    count_s3_files
    echo ""
    
    show_latest_files
    echo ""
    
    check_cdc_lag
    echo ""
    
    echo -e "${BLUE}🔄 Refreshing in ${REFRESH_INTERVAL}s... (Ctrl+C to exit)${NC}"
    sleep $REFRESH_INTERVAL
done 