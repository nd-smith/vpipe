#!/bin/bash
#
# Kafka Pipeline RHEL8 Deployment Installation Script
#
# This script sets up the kafka-pipeline workers as systemd services on RHEL8.
# It handles user creation, directory setup, file deployment, and service installation.
#
# Usage:
#   sudo ./install.sh [--install-dir /opt/kafka-pipeline] [--user kafka-pipeline]
#

set -e  # Exit on error

# Default configuration
INSTALL_DIR="/opt/kafka-pipeline"
SERVICE_USER="kafka-pipeline"
SERVICE_GROUP="kafka-pipeline"
SYSTEMD_DIR="/etc/systemd/system"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --install-dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --user)
            SERVICE_USER="$2"
            SERVICE_GROUP="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --install-dir DIR    Installation directory (default: /opt/kafka-pipeline)"
            echo "  --user USER          Service user (default: kafka-pipeline)"
            echo "  --help               Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run with --help for usage information"
            exit 1
            ;;
    esac
done

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo -e "${RED}Error: This script must be run as root (use sudo)${NC}"
   exit 1
fi

echo -e "${GREEN}=== Kafka Pipeline RHEL8 Installation ===${NC}"
echo ""
echo "Installation directory: $INSTALL_DIR"
echo "Service user: $SERVICE_USER"
echo "Project root: $PROJECT_ROOT"
echo ""

# Step 1: Create service user and group
echo -e "${YELLOW}[1/8] Creating service user and group...${NC}"
if id "$SERVICE_USER" &>/dev/null; then
    echo "User $SERVICE_USER already exists"
else
    useradd --system --no-create-home --shell /bin/false "$SERVICE_USER"
    echo "Created user: $SERVICE_USER"
fi

# Step 2: Create directory structure
echo -e "${YELLOW}[2/8] Creating directory structure...${NC}"
mkdir -p "$INSTALL_DIR"/{src,config,logs,venv}
echo "Created directories under $INSTALL_DIR"

# Step 3: Copy application files
echo -e "${YELLOW}[3/8] Copying application files...${NC}"
if [ -d "$PROJECT_ROOT/src" ]; then
    echo "Copying source code..."
    rsync -a --delete "$PROJECT_ROOT/src/" "$INSTALL_DIR/src/"
    echo "Source code copied"
else
    echo -e "${RED}Error: Source directory not found at $PROJECT_ROOT/src${NC}"
    exit 1
fi

# Copy config files
if [ -d "$PROJECT_ROOT/config" ]; then
    echo "Copying configuration files..."
    rsync -a "$PROJECT_ROOT/config/" "$INSTALL_DIR/config/"
    echo "Configuration copied"
else
    echo -e "${YELLOW}Warning: Config directory not found at $PROJECT_ROOT/config${NC}"
fi

# Copy .env file if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "Copying .env file..."
    cp "$PROJECT_ROOT/.env" "$INSTALL_DIR/config/.env"
    chmod 600 "$INSTALL_DIR/config/.env"
    echo ".env file copied (permissions: 600)"
else
    echo -e "${YELLOW}Warning: .env file not found at $PROJECT_ROOT/.env${NC}"
    echo "You will need to create $INSTALL_DIR/config/.env manually"
fi

# Step 4: Set up Python virtual environment
echo -e "${YELLOW}[4/8] Setting up Python virtual environment...${NC}"
if [ -d "$INSTALL_DIR/venv" ] && [ -f "$INSTALL_DIR/venv/bin/python" ]; then
    echo "Virtual environment already exists, skipping creation"
else
    python3 -m venv "$INSTALL_DIR/venv"
    echo "Virtual environment created"
fi

# Install dependencies
if [ -f "$PROJECT_ROOT/requirements.txt" ]; then
    echo "Installing Python dependencies..."
    "$INSTALL_DIR/venv/bin/pip" install --upgrade pip
    "$INSTALL_DIR/venv/bin/pip" install -r "$PROJECT_ROOT/requirements.txt"
    echo "Dependencies installed"
else
    echo -e "${YELLOW}Warning: requirements.txt not found${NC}"
fi

# Step 5: Set ownership and permissions
echo -e "${YELLOW}[5/8] Setting ownership and permissions...${NC}"
chown -R "$SERVICE_USER:$SERVICE_GROUP" "$INSTALL_DIR"
chmod -R 755 "$INSTALL_DIR"
chmod 600 "$INSTALL_DIR/config/.env" 2>/dev/null || true
echo "Ownership set to $SERVICE_USER:$SERVICE_GROUP"

# Step 6: Install systemd service files
echo -e "${YELLOW}[6/8] Installing systemd service files...${NC}"
SERVICE_COUNT=0
for service_file in "$SCRIPT_DIR/systemd"/*.service; do
    if [ -f "$service_file" ]; then
        service_name=$(basename "$service_file")
        echo "Installing $service_name..."
        cp "$service_file" "$SYSTEMD_DIR/$service_name"
        chmod 644 "$SYSTEMD_DIR/$service_name"
        SERVICE_COUNT=$((SERVICE_COUNT + 1))
    fi
done
echo "Installed $SERVICE_COUNT service files"

# Step 7: Reload systemd
echo -e "${YELLOW}[7/8] Reloading systemd daemon...${NC}"
systemctl daemon-reload
echo "Systemd daemon reloaded"

# Step 8: Display summary and next steps
echo -e "${YELLOW}[8/8] Installation complete!${NC}"
echo ""
echo -e "${GREEN}=== Installation Summary ===${NC}"
echo "Installation directory: $INSTALL_DIR"
echo "Service user: $SERVICE_USER"
echo "Services installed: $SERVICE_COUNT"
echo ""
echo -e "${GREEN}=== Next Steps ===${NC}"
echo ""
echo "1. Configure environment variables:"
echo "   sudo nano $INSTALL_DIR/config/.env"
echo ""
echo "2. Start specific workers (examples):"
echo "   sudo systemctl start kafka-pipeline-xact-poller"
echo "   sudo systemctl start kafka-pipeline-xact-event-ingester"
echo "   sudo systemctl start kafka-pipeline-xact-download@{1..4}"
echo ""
echo "3. Enable workers to start on boot:"
echo "   sudo systemctl enable kafka-pipeline-xact-poller"
echo "   sudo systemctl enable kafka-pipeline-xact-download@{1..4}"
echo ""
echo "4. Check worker status:"
echo "   sudo systemctl status kafka-pipeline-xact-poller"
echo "   sudo journalctl -u kafka-pipeline-xact-download@1 -f"
echo ""
echo "5. Use the management script for easier operations:"
echo "   sudo $SCRIPT_DIR/manage-workers.sh status"
echo "   sudo $SCRIPT_DIR/manage-workers.sh start xact-download 4"
echo ""
echo -e "${GREEN}Installation completed successfully!${NC}"
