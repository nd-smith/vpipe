#!/bin/bash
# Stop VPipe Simulation Environment
#
# Usage:
#   ./scripts/stop_simulation.sh           # Stop but keep data
#   ./scripts/stop_simulation.sh --clean   # Stop and remove all data

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

CLEAN=false

for arg in "$@"; do
    case $arg in
        --clean)
            CLEAN=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --clean   Remove all data (volumes, logs, simulation files)"
            echo "  --help    Show this help message"
            exit 0
            ;;
    esac
done

echo -e "${YELLOW}Stopping simulation environment...${NC}"

if [ "$CLEAN" = true ]; then
    docker-compose -f docker-compose.simulation.yml down -v
    echo -e "${YELLOW}Cleaning up data...${NC}"
    rm -rf logs/
    rm -rf /tmp/vpipe_simulation
    echo -e "${GREEN}✓ Simulation stopped and cleaned${NC}"
else
    docker-compose -f docker-compose.simulation.yml down
    echo -e "${GREEN}✓ Simulation stopped (data preserved)${NC}"
    echo ""
    echo "Data locations:"
    echo "  - Kafka data:        docker volume 'kafka_simulation_data'"
    echo "  - Simulation files:  /tmp/vpipe_simulation/"
    echo "  - Logs:              $PROJECT_ROOT/logs/"
    echo ""
    echo "To clean all data: $0 --clean"
fi
