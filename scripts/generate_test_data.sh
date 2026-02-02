#!/bin/bash
# Generate test data for simulation mode
# This script runs the dummy data producer in simulation mode

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Ensure simulation mode is enabled
export SIMULATION_MODE=true
export PYTHONPATH="$PROJECT_ROOT/src"

# Default values (can be overridden by environment variables)
DOMAINS="${DOMAINS:-claimx,xact}"
EVENTS_PER_MINUTE="${EVENTS_PER_MINUTE:-60}"
MAX_EVENTS="${MAX_EVENTS:-100}"
PLUGIN_PROFILE="${PLUGIN_PROFILE:-}"

# Color output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Pcesdopodappv1 Test Data Generator${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Domains: ${GREEN}$DOMAINS${NC}"
echo -e "Events per minute: ${GREEN}$EVENTS_PER_MINUTE${NC}"
echo -e "Max events: ${GREEN}$MAX_EVENTS${NC}"
if [[ -n "$PLUGIN_PROFILE" ]]; then
    echo -e "Plugin profile: ${GREEN}$PLUGIN_PROFILE${NC}"
fi
echo ""

# Build command
CMD="python -m pipeline.simulation.dummy_producer"
CMD="$CMD --domains $DOMAINS"
CMD="$CMD --events-per-minute $EVENTS_PER_MINUTE"
CMD="$CMD --max-events $MAX_EVENTS"

if [[ -n "$PLUGIN_PROFILE" ]]; then
    CMD="$CMD --plugin-profile $PLUGIN_PROFILE"
fi

# Run the dummy producer
echo -e "${YELLOW}Generating test data...${NC}"
echo ""

if $CMD; then
    echo ""
    echo -e "${GREEN}✓ Test data generation complete${NC}"
    echo ""
    echo "To verify data:"
    echo "  - Check Kafka topics"
    echo "  - Inspect simulation storage: ls -lh /tmp/pcesdopodappv1_simulation/"
else
    echo ""
    echo -e "${RED}✗ Test data generation failed${NC}"
    exit 1
fi
