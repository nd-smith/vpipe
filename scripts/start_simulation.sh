#!/bin/bash
# Start Pcesdopodappv1 Simulation Environment
#
# This script builds and launches the complete simulation environment:
# - Kafka infrastructure
# - Dummy data producer (1000 events/min)
# - File server
# - All pipeline workers
#
# Usage:
#   ./scripts/start_simulation.sh          # Start everything
#   ./scripts/start_simulation.sh --build  # Rebuild images first
#   ./scripts/start_simulation.sh --clean  # Clean everything and restart

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Pcesdopodappv1 Simulation Environment${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Parse arguments
BUILD=false
CLEAN=false

for arg in "$@"; do
    case $arg in
        --build)
            BUILD=true
            shift
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --build   Rebuild Docker images before starting"
            echo "  --clean   Stop and remove all containers, volumes, and rebuild"
            echo "  --help    Show this help message"
            echo ""
            echo "Simulation generates 1000 events/min for both ClaimX and XACT domains"
            exit 0
            ;;
    esac
done

# Clean if requested
if [ "$CLEAN" = true ]; then
    echo -e "${YELLOW}Cleaning up existing simulation environment...${NC}"
    docker-compose -f scripts/docker/docker-compose.simulation.yml down -v
    rm -rf logs/
    rm -rf /tmp/pcesdopodappv1_simulation
    echo -e "${GREEN}✓ Cleanup complete${NC}"
    echo ""
fi

# Build images if requested or if they don't exist
if [ "$BUILD" = true ] || ! docker images | grep -q "kafka-pipeline"; then
    echo -e "${YELLOW}Building Docker images...${NC}"
    docker-compose -f scripts/docker/docker-compose.simulation.yml build
    echo -e "${GREEN}✓ Build complete${NC}"
    echo ""
fi

# Create directories
echo -e "${YELLOW}Creating directories...${NC}"
mkdir -p logs
mkdir -p /tmp/pcesdopodappv1_simulation
echo -e "${GREEN}✓ Directories created${NC}"
echo ""

# Start all services (Kafka will start first, workers will wait for it to be healthy)
echo -e "${YELLOW}Starting all simulation services...${NC}"
docker-compose -f scripts/docker/docker-compose.simulation.yml --profile simulation up -d
echo ""

# Wait for Kafka to be healthy
echo "Waiting for Kafka to be healthy..."
timeout 120 bash -c 'until [ "$(docker inspect -f "{{.State.Health.Status}}" kafka-simulation 2>/dev/null)" = "healthy" ]; do sleep 2; done' || {
    echo -e "${RED}✗ Kafka failed to start within 120 seconds${NC}"
    echo "Kafka status: $(docker inspect -f '{{.State.Health.Status}}' kafka-simulation 2>/dev/null || echo 'container not found')"
    docker-compose -f scripts/docker/docker-compose.simulation.yml logs kafka | tail -50
    exit 1
}
echo -e "${GREEN}✓ Kafka is ready${NC}"
echo -e "${GREEN}✓ Workers are starting (they wait for Kafka health check)${NC}"
echo ""

# Show status
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Simulation Environment Running${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Services:"
echo "  - Kafka:        http://localhost:9094 (bootstrap)"
echo "  - Kafka UI:     http://localhost:8080"
echo "  - File Server:  http://localhost:8765/health"
echo ""
echo "Event Generation:"
echo "  - Rate:         1000 events/min per domain"
echo "  - Domains:      ClaimX, XACT"
echo "  - Max Events:   100,000"
echo ""
echo "Workers Running:"
docker-compose -f scripts/docker/docker-compose.simulation.yml ps | grep -E "ingester|enricher|download|upload|delta|retry|entity|producer|file-server" || echo "  (Starting up...)"
echo ""
echo "Useful Commands:"
echo "  View logs:      docker-compose -f scripts/docker/docker-compose.simulation.yml logs -f"
echo "  View producer:  docker-compose -f scripts/docker/docker-compose.simulation.yml logs -f dummy-producer"
echo "  Stop all:       docker-compose -f scripts/docker/docker-compose.simulation.yml down"
echo "  Clean all:      docker-compose -f scripts/docker/docker-compose.simulation.yml down -v"
echo ""
echo "Simulation data stored at: /tmp/pcesdopodappv1_simulation/"
echo "Logs stored at: $PROJECT_ROOT/logs/"
echo ""
echo -e "${GREEN}Simulation is running! Press Ctrl+C to stop.${NC}"
echo ""
