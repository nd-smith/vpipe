#!/bin/bash
#
# Kafka Container Setup Script for RHEL8 with Podman
#
# Sets up Kafka as a containerized service managed by systemd
#

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.kafka.yml"

# Check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        echo -e "${RED}Error: This script must be run as root (use sudo)${NC}"
        exit 1
    fi
}

# Install Podman and podman-compose
install_podman() {
    echo -e "${YELLOW}[1/4] Installing Podman...${NC}"

    if command -v podman &> /dev/null; then
        echo "Podman already installed: $(podman --version)"
    else
        dnf install -y podman
        echo "Podman installed successfully"
    fi

    # Install podman-compose
    if command -v podman-compose &> /dev/null; then
        echo "podman-compose already installed: $(podman-compose --version)"
    else
        dnf install -y podman-compose
        echo "podman-compose installed successfully"
    fi
}

# Start Kafka with podman-compose
start_kafka() {
    echo -e "${YELLOW}[2/4] Starting Kafka container...${NC}"

    if [ ! -f "$COMPOSE_FILE" ]; then
        echo -e "${RED}Error: docker-compose.kafka.yml not found at $COMPOSE_FILE${NC}"
        exit 1
    fi

    cd "$SCRIPT_DIR"

    # Start Kafka and init container
    podman-compose -f docker-compose.kafka.yml up -d

    echo "Kafka container started"

    # Wait for health check
    echo "Waiting for Kafka to become healthy..."
    for i in {1..30}; do
        if podman healthcheck run kafka-pipeline-local &>/dev/null; then
            echo -e "${GREEN}Kafka is healthy!${NC}"
            break
        fi
        echo -n "."
        sleep 2
    done
}

# Generate systemd service
create_systemd_service() {
    echo -e "${YELLOW}[3/4] Creating systemd service...${NC}"

    # Generate systemd unit file
    cd /tmp
    podman generate systemd --name kafka-pipeline-local --files --new

    # Move to systemd directory
    mv container-kafka-pipeline-local.service /etc/systemd/system/

    # Reload systemd
    systemctl daemon-reload

    # Enable service
    systemctl enable container-kafka-pipeline-local.service

    echo "Systemd service created and enabled"
}

# Verify installation
verify_installation() {
    echo -e "${YELLOW}[4/4] Verifying installation...${NC}"

    # Check container status
    echo -n "Container status: "
    if podman ps | grep -q kafka-pipeline-local; then
        echo -e "${GREEN}Running${NC}"
    else
        echo -e "${RED}Not running${NC}"
        exit 1
    fi

    # Check if Kafka is responding
    echo -n "Kafka connectivity: "
    if podman exec kafka-pipeline-local /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list &>/dev/null; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}Failed${NC}"
        exit 1
    fi

    # List topics
    echo ""
    echo "Available topics:"
    podman exec kafka-pipeline-local /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

    echo ""
    echo -e "${GREEN}=== Installation Complete ===${NC}"
    echo ""
    echo "Kafka is running on:"
    echo "  - Internal (containers): kafka:9092"
    echo "  - External (host/workers): localhost:9094"
    echo ""
    echo "Management commands:"
    echo "  sudo systemctl status container-kafka-pipeline-local"
    echo "  sudo systemctl restart container-kafka-pipeline-local"
    echo "  sudo journalctl -u container-kafka-pipeline-local -f"
    echo ""
    echo "Worker configuration (.env):"
    echo "  KAFKA_BOOTSTRAP_SERVERS=localhost:9094"
}

# Show usage
show_usage() {
    echo "Usage: $0 [install|start|stop|restart|status|logs]"
    echo ""
    echo "Commands:"
    echo "  install   - Install Podman and set up Kafka with systemd"
    echo "  start     - Start Kafka container"
    echo "  stop      - Stop Kafka container"
    echo "  restart   - Restart Kafka container"
    echo "  status    - Show Kafka status"
    echo "  logs      - Show Kafka logs"
    echo "  topics    - List all Kafka topics"
}

# Main script
COMMAND=${1:-install}

case "$COMMAND" in
    install)
        check_root
        echo -e "${GREEN}=== Kafka Container Setup for RHEL8 ===${NC}"
        echo ""
        install_podman
        start_kafka
        create_systemd_service
        verify_installation
        ;;
    start)
        check_root
        echo "Starting Kafka..."
        systemctl start container-kafka-pipeline-local
        echo -e "${GREEN}Kafka started${NC}"
        ;;
    stop)
        check_root
        echo "Stopping Kafka..."
        systemctl stop container-kafka-pipeline-local
        echo -e "${YELLOW}Kafka stopped${NC}"
        ;;
    restart)
        check_root
        echo "Restarting Kafka..."
        systemctl restart container-kafka-pipeline-local
        echo -e "${GREEN}Kafka restarted${NC}"
        ;;
    status)
        systemctl status container-kafka-pipeline-local
        echo ""
        podman ps | grep kafka-pipeline-local
        ;;
    logs)
        echo "Following Kafka logs (Ctrl+C to exit)..."
        journalctl -u container-kafka-pipeline-local -f
        ;;
    topics)
        echo "Kafka topics:"
        podman exec kafka-pipeline-local /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
        ;;
    *)
        show_usage
        exit 1
        ;;
esac
