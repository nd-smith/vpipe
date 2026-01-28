#!/bin/bash
# Run full simulation pipeline
# Starts file server, workers, and dummy producer for end-to-end testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

export SIMULATION_MODE=true
export PYTHONPATH="$PROJECT_ROOT/src"
export LOG_LEVEL="${LOG_LEVEL:-INFO}"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
DOMAIN="${1:-claimx}"
EVENT_COUNT="${2:-50}"
CLEAN="${CLEAN:-yes}"

# Validate domain
if [[ "$DOMAIN" != "claimx" && "$DOMAIN" != "xact" ]]; then
    echo -e "${RED}Error: Domain must be 'claimx' or 'xact'${NC}"
    echo "Usage: $0 [claimx|xact] [event_count]"
    exit 1
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Pcesdopodappv1 Simulation Mode${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Domain: ${GREEN}$DOMAIN${NC}"
echo -e "Event Count: ${GREEN}$EVENT_COUNT${NC}"
echo -e "Project Root: $PROJECT_ROOT"
echo ""

# Array to store background PIDs
PIDS=()

# Function to cleanup on exit
cleanup() {
    echo ""
    echo -e "${YELLOW}Stopping all workers...${NC}"

    # Kill all background processes
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done

    # Wait a moment for graceful shutdown
    sleep 2

    # Force kill any remaining
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    done

    echo -e "${GREEN}Cleanup complete${NC}"
}

trap cleanup EXIT INT TERM

# Clean simulation directory if requested
if [[ "$CLEAN" == "yes" ]]; then
    echo -e "${YELLOW}Cleaning simulation directory...${NC}"
    "$SCRIPT_DIR/cleanup_simulation_data.sh" --yes
    echo ""
fi

# Check if Kafka is running
echo -e "${YELLOW}Checking Kafka connectivity...${NC}"
if ! nc -z localhost 9092 2>/dev/null; then
    echo -e "${RED}Error: Kafka not reachable at localhost:9092${NC}"
    echo "Start Kafka with: docker-compose -f scripts/docker/docker-compose.kafka.yml up -d"
    exit 1
fi
echo -e "${GREEN}✓ Kafka is running${NC}"
echo ""

# Start file server
echo -e "${YELLOW}Starting dummy file server...${NC}"
python -m kafka_pipeline.common.dummy.file_server > /tmp/pcesdopodappv1_file_server.log 2>&1 &
FILE_SERVER_PID=$!
PIDS+=($FILE_SERVER_PID)
echo -e "${GREEN}✓ File server started (PID: $FILE_SERVER_PID)${NC}"
sleep 3

# Verify file server is responsive
if ! curl -s http://localhost:8765/health > /dev/null 2>&1; then
    echo -e "${RED}Error: File server not responding${NC}"
    echo "Check logs: tail -f /tmp/pcesdopodappv1_file_server.log"
    exit 1
fi
echo -e "${GREEN}✓ File server is responding${NC}"
echo ""

# Start workers based on domain
echo -e "${YELLOW}Starting $DOMAIN pipeline workers...${NC}"

if [[ "$DOMAIN" == "claimx" ]]; then
    # ClaimX pipeline

    echo "  Starting claimx-ingester..."
    python -m kafka_pipeline --worker claimx-ingester > /tmp/pcesdopodappv1_claimx_ingester.log 2>&1 &
    PIDS+=($!)
    sleep 2

    echo "  Starting claimx-enricher..."
    python -m kafka_pipeline --worker claimx-enricher --simulation-mode > /tmp/pcesdopodappv1_claimx_enricher.log 2>&1 &
    PIDS+=($!)
    sleep 2

    echo "  Starting claimx-downloader..."
    python -m kafka_pipeline --worker claimx-downloader > /tmp/pcesdopodappv1_claimx_downloader.log 2>&1 &
    PIDS+=($!)
    sleep 2

    echo "  Starting claimx-uploader..."
    python -m kafka_pipeline --worker claimx-uploader --simulation-mode > /tmp/pcesdopodappv1_claimx_uploader.log 2>&1 &
    PIDS+=($!)
    sleep 2

    echo "  Starting claimx-entity-writer..."
    python -m kafka_pipeline --worker claimx-entity-writer > /tmp/pcesdopodappv1_claimx_entity_writer.log 2>&1 &
    PIDS+=($!)
    sleep 2

else
    # XACT pipeline

    echo "  Starting xact-local-ingester..."
    python -m kafka_pipeline --worker xact-local-ingester > /tmp/pcesdopodappv1_xact_ingester.log 2>&1 &
    PIDS+=($!)
    sleep 2

    echo "  Starting xact-enricher..."
    python -m kafka_pipeline --worker xact-enricher --simulation-mode > /tmp/pcesdopodappv1_xact_enricher.log 2>&1 &
    PIDS+=($!)
    sleep 2

    echo "  Starting xact-download..."
    python -m kafka_pipeline --worker xact-download > /tmp/pcesdopodappv1_xact_download.log 2>&1 &
    PIDS+=($!)
    sleep 2

    echo "  Starting xact-upload..."
    python -m kafka_pipeline --worker xact-upload --simulation-mode > /tmp/pcesdopodappv1_xact_upload.log 2>&1 &
    PIDS+=($!)
    sleep 2
fi

echo -e "${GREEN}✓ All workers started${NC}"
echo ""

# Wait for workers to initialize
echo -e "${YELLOW}Waiting for workers to initialize (10 seconds)...${NC}"
sleep 10
echo ""

# Run dummy producer
echo -e "${YELLOW}Running dummy data producer...${NC}"
echo -e "  Domain: $DOMAIN"
echo -e "  Events: $EVENT_COUNT"
echo ""

# Run dummy source using new simulation module and wait for completion
if python -m kafka_pipeline.simulation.dummy_producer \
    --domains "$DOMAIN" \
    --events-per-minute 120 \
    --max-events "$EVENT_COUNT" > /tmp/pcesdopodappv1_dummy_source.log 2>&1; then
    echo -e "${GREEN}✓ Dummy producer completed${NC}"
else
    echo -e "${RED}✗ Dummy producer failed${NC}"
    echo "Check logs: tail -f /tmp/pcesdopodappv1_dummy_source.log"
fi
echo ""

# Wait for pipeline processing
WAIT_TIME=30
echo -e "${YELLOW}Waiting for pipeline to process events ($WAIT_TIME seconds)...${NC}"
sleep $WAIT_TIME
echo ""

# Show results
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Simulation Results${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check uploaded files
echo -e "${YELLOW}Uploaded Files:${NC}"
STORAGE_PATH="/tmp/pcesdopodappv1_simulation/$DOMAIN"
if [[ -d "$STORAGE_PATH" ]]; then
    FILE_COUNT=$(find "$STORAGE_PATH" -type f 2>/dev/null | wc -l)
    echo -e "  Location: $STORAGE_PATH"
    echo -e "  Count: ${GREEN}$FILE_COUNT files${NC}"

    if [[ $FILE_COUNT -gt 0 ]]; then
        echo ""
        echo "  Recent files:"
        find "$STORAGE_PATH" -type f -printf "    %f (%s bytes)\n" 2>/dev/null | head -5
    fi
else
    echo -e "  ${YELLOW}No files uploaded (storage path not found)${NC}"
fi
echo ""

# Check Delta tables
echo -e "${YELLOW}Delta Tables:${NC}"
DELTA_PATH="/tmp/pcesdopodappv1_simulation/delta"
if [[ -d "$DELTA_PATH" ]]; then
    echo -e "  Location: $DELTA_PATH"

    TABLE_COUNT=0
    for table_dir in "$DELTA_PATH"/*; do
        if [[ -d "$table_dir/_delta_log" ]]; then
            table_name=$(basename "$table_dir")
            TABLE_COUNT=$((TABLE_COUNT + 1))
            echo -e "  ${GREEN}✓${NC} $table_name"
        fi
    done

    if [[ $TABLE_COUNT -eq 0 ]]; then
        echo -e "  ${YELLOW}No Delta tables found${NC}"
    else
        echo ""
        echo -e "  Total: ${GREEN}$TABLE_COUNT tables${NC}"
    fi
else
    echo -e "  ${YELLOW}No Delta tables (delta path not found)${NC}"
fi
echo ""

# Check for errors in logs
echo -e "${YELLOW}Error Summary:${NC}"
ERROR_COUNT=$(grep -h ERROR /tmp/pcesdopodappv1_*.log 2>/dev/null | wc -l)
if [[ $ERROR_COUNT -gt 0 ]]; then
    echo -e "  ${RED}Found $ERROR_COUNT errors in logs${NC}"
    echo ""
    echo "  Recent errors:"
    grep -h ERROR /tmp/pcesdopodappv1_*.log 2>/dev/null | tail -5 | sed 's/^/    /'
    echo ""
    echo "  View full logs: tail -f /tmp/pcesdopodappv1_*.log"
else
    echo -e "  ${GREEN}✓ No errors found${NC}"
fi
echo ""

# Run verification script if available
if [[ -f "$SCRIPT_DIR/verify_simulation.py" ]]; then
    echo -e "${YELLOW}Running verification script...${NC}"
    echo ""
    python "$SCRIPT_DIR/verify_simulation.py"
    echo ""
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Simulation Complete${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "To inspect data:"
echo "  - Files: ls -lah $STORAGE_PATH"
echo "  - Delta: python scripts/inspect_simulation_delta.py"
echo "  - Logs: tail -f /tmp/pcesdopodappv1_*.log"
echo ""
echo "To clean up:"
echo "  - ./scripts/cleanup_simulation_data.sh --yes"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop workers and exit${NC}"

# Keep running until interrupted
wait
