#!/bin/bash
#
# Kafka Pipeline Worker Management Script
#
# Simplified interface for managing kafka-pipeline systemd services
#
# Usage:
#   ./manage-workers.sh <command> [worker] [instances]
#
# Examples:
#   ./manage-workers.sh status                    # Show all worker statuses
#   ./manage-workers.sh start xact-download 4     # Start 4 download workers
#   ./manage-workers.sh stop xact-download 2      # Stop download worker instance 2
#   ./manage-workers.sh restart claimx-enricher 3 # Restart 3 enricher instances
#   ./manage-workers.sh logs xact-download 1      # Show logs for instance 1
#   ./manage-workers.sh enable xact-poller        # Enable poller on boot
#

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Available workers
XACT_WORKERS=(
    "xact-poller"
    "xact-event-ingester"
    "xact-enricher"
    "xact-download"
    "xact-upload"
    "xact-delta-writer"
    "xact-result-processor"
    "xact-retry-scheduler"
)

CLAIMX_WORKERS=(
    "claimx-poller"
    "claimx-ingester"
    "claimx-enricher"
    "claimx-downloader"
    "claimx-uploader"
    "claimx-delta-writer"
    "claimx-entity-writer"
    "claimx-result-processor"
    "claimx-retry-scheduler"
)

# Workers that support multiple instances (use @ template)
MULTI_INSTANCE_WORKERS=(
    "xact-enricher"
    "xact-download"
    "xact-upload"
    "xact-delta-writer"
    "claimx-enricher"
    "claimx-downloader"
    "claimx-uploader"
)

# Check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        echo -e "${RED}Error: This script must be run as root (use sudo)${NC}"
        exit 1
    fi
}

# Check if worker supports multiple instances
is_multi_instance() {
    local worker=$1
    for w in "${MULTI_INSTANCE_WORKERS[@]}"; do
        if [[ "$w" == "$worker" ]]; then
            return 0
        fi
    done
    return 1
}

# Get service name for worker
get_service_name() {
    local worker=$1
    local instance=${2:-}

    if is_multi_instance "$worker" && [[ -n "$instance" ]]; then
        echo "kafka-pipeline-${worker}@${instance}.service"
    else
        echo "kafka-pipeline-${worker}.service"
    fi
}

# Show usage
show_usage() {
    echo "Usage: $0 <command> [worker] [instances]"
    echo ""
    echo "Commands:"
    echo "  status                       Show status of all workers"
    echo "  start <worker> [count]       Start worker (with optional instance count)"
    echo "  stop <worker> [instance]     Stop worker (or specific instance)"
    echo "  restart <worker> [count]     Restart worker (with optional instance count)"
    echo "  enable <worker> [count]      Enable worker to start on boot"
    echo "  disable <worker> [instance]  Disable worker from starting on boot"
    echo "  logs <worker> [instance]     Show logs for worker"
    echo "  pid <worker> [instance]      Show PID for worker"
    echo "  list                         List all available workers"
    echo ""
    echo "Workers supporting multiple instances:"
    printf "  %s\n" "${MULTI_INSTANCE_WORKERS[@]}"
    echo ""
    echo "Examples:"
    echo "  $0 status"
    echo "  $0 start xact-download 4"
    echo "  $0 stop xact-download 2"
    echo "  $0 restart xact-poller"
    echo "  $0 logs xact-download 1"
    echo "  $0 pid claimx-enricher 2"
}

# Show status of all workers
show_status() {
    echo -e "${GREEN}=== Kafka Pipeline Worker Status ===${NC}"
    echo ""

    echo -e "${BLUE}XACT Workers:${NC}"
    for worker in "${XACT_WORKERS[@]}"; do
        if is_multi_instance "$worker"; then
            # Check for running instances
            local count=0
            for i in {1..10}; do
                if systemctl is-active --quiet "kafka-pipeline-${worker}@${i}.service" 2>/dev/null; then
                    ((count++))
                fi
            done
            if [ $count -gt 0 ]; then
                echo -e "  ${worker}: ${GREEN}${count} instance(s) running${NC}"
            else
                echo -e "  ${worker}: ${YELLOW}not running${NC}"
            fi
        else
            if systemctl is-active --quiet "kafka-pipeline-${worker}.service"; then
                echo -e "  ${worker}: ${GREEN}running${NC}"
            else
                echo -e "  ${worker}: ${YELLOW}not running${NC}"
            fi
        fi
    done

    echo ""
    echo -e "${BLUE}ClaimX Workers:${NC}"
    for worker in "${CLAIMX_WORKERS[@]}"; do
        if is_multi_instance "$worker"; then
            local count=0
            for i in {1..10}; do
                if systemctl is-active --quiet "kafka-pipeline-${worker}@${i}.service" 2>/dev/null; then
                    ((count++))
                fi
            done
            if [ $count -gt 0 ]; then
                echo -e "  ${worker}: ${GREEN}${count} instance(s) running${NC}"
            else
                echo -e "  ${worker}: ${YELLOW}not running${NC}"
            fi
        else
            if systemctl is-active --quiet "kafka-pipeline-${worker}.service"; then
                echo -e "  ${worker}: ${GREEN}running${NC}"
            else
                echo -e "  ${worker}: ${YELLOW}not running${NC}"
            fi
        fi
    done
}

# Start worker
start_worker() {
    local worker=$1
    local count=${2:-1}

    if is_multi_instance "$worker"; then
        echo -e "${GREEN}Starting $count instance(s) of $worker...${NC}"
        for i in $(seq 1 "$count"); do
            local service=$(get_service_name "$worker" "$i")
            systemctl start "$service"
            echo "  Started: $service"
        done
    else
        local service=$(get_service_name "$worker")
        echo -e "${GREEN}Starting $worker...${NC}"
        systemctl start "$service"
        echo "  Started: $service"
    fi
}

# Stop worker
stop_worker() {
    local worker=$1
    local instance=${2:-}

    if is_multi_instance "$worker" && [[ -n "$instance" ]]; then
        # Stop specific instance
        local service=$(get_service_name "$worker" "$instance")
        echo -e "${YELLOW}Stopping $service...${NC}"
        systemctl stop "$service"
        echo "  Stopped: $service"
    elif is_multi_instance "$worker"; then
        # Stop all instances
        echo -e "${YELLOW}Stopping all instances of $worker...${NC}"
        systemctl stop "kafka-pipeline-${worker}@*.service" 2>/dev/null || true
        echo "  Stopped all instances"
    else
        local service=$(get_service_name "$worker")
        echo -e "${YELLOW}Stopping $service...${NC}"
        systemctl stop "$service"
        echo "  Stopped: $service"
    fi
}

# Restart worker
restart_worker() {
    local worker=$1
    local count=${2:-1}

    stop_worker "$worker"
    sleep 1
    start_worker "$worker" "$count"
}

# Enable worker
enable_worker() {
    local worker=$1
    local count=${2:-1}

    if is_multi_instance "$worker"; then
        echo -e "${GREEN}Enabling $count instance(s) of $worker...${NC}"
        for i in $(seq 1 "$count"); do
            local service=$(get_service_name "$worker" "$i")
            systemctl enable "$service"
            echo "  Enabled: $service"
        done
    else
        local service=$(get_service_name "$worker")
        echo -e "${GREEN}Enabling $worker...${NC}"
        systemctl enable "$service"
        echo "  Enabled: $service"
    fi
}

# Disable worker
disable_worker() {
    local worker=$1
    local instance=${2:-}

    if is_multi_instance "$worker" && [[ -n "$instance" ]]; then
        local service=$(get_service_name "$worker" "$instance")
        echo -e "${YELLOW}Disabling $service...${NC}"
        systemctl disable "$service"
        echo "  Disabled: $service"
    elif is_multi_instance "$worker"; then
        echo -e "${YELLOW}Disabling all instances of $worker...${NC}"
        systemctl disable "kafka-pipeline-${worker}@*.service" 2>/dev/null || true
        echo "  Disabled all instances"
    else
        local service=$(get_service_name "$worker")
        echo -e "${YELLOW}Disabling $service...${NC}"
        systemctl disable "$service"
        echo "  Disabled: $service"
    fi
}

# Show logs
show_logs() {
    local worker=$1
    local instance=${2:-}

    if is_multi_instance "$worker" && [[ -n "$instance" ]]; then
        local service=$(get_service_name "$worker" "$instance")
        echo -e "${BLUE}Showing logs for $service (Ctrl+C to exit)...${NC}"
        journalctl -u "$service" -f
    elif is_multi_instance "$worker"; then
        echo -e "${BLUE}Showing logs for all instances of $worker (Ctrl+C to exit)...${NC}"
        journalctl -u "kafka-pipeline-${worker}@*" -f
    else
        local service=$(get_service_name "$worker")
        echo -e "${BLUE}Showing logs for $service (Ctrl+C to exit)...${NC}"
        journalctl -u "$service" -f
    fi
}

# Show PID
show_pid() {
    local worker=$1
    local instance=${2:-}

    if is_multi_instance "$worker" && [[ -n "$instance" ]]; then
        local service=$(get_service_name "$worker" "$instance")
        local pid=$(systemctl show -p MainPID "$service" --value)
        echo "$service: PID $pid"
    elif is_multi_instance "$worker"; then
        echo "PIDs for all instances of $worker:"
        for i in {1..10}; do
            local service="kafka-pipeline-${worker}@${i}.service"
            if systemctl is-active --quiet "$service" 2>/dev/null; then
                local pid=$(systemctl show -p MainPID "$service" --value)
                echo "  Instance $i: PID $pid"
            fi
        done
    else
        local service=$(get_service_name "$worker")
        local pid=$(systemctl show -p MainPID "$service" --value)
        echo "$service: PID $pid"
    fi
}

# List all workers
list_workers() {
    echo -e "${GREEN}=== Available Workers ===${NC}"
    echo ""
    echo -e "${BLUE}XACT Workers:${NC}"
    printf "  %s\n" "${XACT_WORKERS[@]}"
    echo ""
    echo -e "${BLUE}ClaimX Workers:${NC}"
    printf "  %s\n" "${CLAIMX_WORKERS[@]}"
    echo ""
    echo -e "${BLUE}Multi-instance Support:${NC}"
    printf "  %s\n" "${MULTI_INSTANCE_WORKERS[@]}"
}

# Main script
COMMAND=${1:-}

case "$COMMAND" in
    status)
        show_status
        ;;
    start)
        check_root
        if [[ -z "${2:-}" ]]; then
            echo -e "${RED}Error: Worker name required${NC}"
            show_usage
            exit 1
        fi
        start_worker "$2" "${3:-1}"
        ;;
    stop)
        check_root
        if [[ -z "${2:-}" ]]; then
            echo -e "${RED}Error: Worker name required${NC}"
            show_usage
            exit 1
        fi
        stop_worker "$2" "${3:-}"
        ;;
    restart)
        check_root
        if [[ -z "${2:-}" ]]; then
            echo -e "${RED}Error: Worker name required${NC}"
            show_usage
            exit 1
        fi
        restart_worker "$2" "${3:-1}"
        ;;
    enable)
        check_root
        if [[ -z "${2:-}" ]]; then
            echo -e "${RED}Error: Worker name required${NC}"
            show_usage
            exit 1
        fi
        enable_worker "$2" "${3:-1}"
        ;;
    disable)
        check_root
        if [[ -z "${2:-}" ]]; then
            echo -e "${RED}Error: Worker name required${NC}"
            show_usage
            exit 1
        fi
        disable_worker "$2" "${3:-}"
        ;;
    logs)
        if [[ -z "${2:-}" ]]; then
            echo -e "${RED}Error: Worker name required${NC}"
            show_usage
            exit 1
        fi
        show_logs "$2" "${3:-}"
        ;;
    pid)
        if [[ -z "${2:-}" ]]; then
            echo -e "${RED}Error: Worker name required${NC}"
            show_usage
            exit 1
        fi
        show_pid "$2" "${3:-}"
        ;;
    list)
        list_workers
        ;;
    --help|-h|help)
        show_usage
        ;;
    *)
        echo -e "${RED}Error: Unknown command '$COMMAND'${NC}"
        echo ""
        show_usage
        exit 1
        ;;
esac
