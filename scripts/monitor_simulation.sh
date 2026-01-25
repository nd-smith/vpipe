#!/bin/bash
# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

# Monitor simulation pipeline in real-time
# Shows stats from Kafka topics, files, and Delta tables

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

DOMAIN="${1:-claimx}"
REFRESH_INTERVAL="${2:-5}"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Simulation Pipeline Monitor${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Domain: ${GREEN}$DOMAIN${NC}"
echo -e "Refresh: ${GREEN}${REFRESH_INTERVAL}s${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

while true; do
    clear
    echo -e "${BLUE}VPipe Simulation Monitor - $DOMAIN${NC}"
    echo -e "$(date)"
    echo ""

    # Kafka Topics
    echo -e "${YELLOW}Kafka Topics:${NC}"
    topics=(
        "$DOMAIN.events.raw"
        "$DOMAIN.enrichment.pending"
        "$DOMAIN.downloads.pending"
        "$DOMAIN.downloads.cached"
        "$DOMAIN.downloads.results"
    )

    if [[ "$DOMAIN" == "claimx" ]]; then
        topics+=("$DOMAIN.entities.rows")
    fi

    for topic in "${topics[@]}"; do
        # Get message count (rough estimate using partition end offsets)
        count=$(kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:9092 \
            --topic "$topic" \
            --time -1 2>/dev/null | \
            awk -F: '{sum += $3} END {print sum}')

        if [[ -n "$count" && "$count" -gt 0 ]]; then
            echo -e "  ${GREEN}✓${NC} $topic: $count messages"
        else
            echo -e "  ${YELLOW}-${NC} $topic: 0 messages"
        fi
    done
    echo ""

    # Consumer Groups
    echo -e "${YELLOW}Consumer Groups:${NC}"
    groups=$(kafka-consumer-groups --bootstrap-server localhost:9092 --list 2>/dev/null | grep "$DOMAIN")
    if [[ -n "$groups" ]]; then
        for group in $groups; do
            # Get lag
            lag=$(kafka-consumer-groups --bootstrap-server localhost:9092 \
                --describe --group "$group" 2>/dev/null | \
                awk 'NR>1 {sum+=$5} END {print sum}')

            if [[ -n "$lag" && "$lag" -gt 0 ]]; then
                echo -e "  ${YELLOW}⚠${NC}  $group: lag $lag"
            else
                echo -e "  ${GREEN}✓${NC} $group: no lag"
            fi
        done
    else
        echo -e "  ${YELLOW}No active consumer groups${NC}"
    fi
    echo ""

    # Files
    echo -e "${YELLOW}Local Storage:${NC}"
    STORAGE_PATH="/tmp/vpipe_simulation/$DOMAIN"
    if [[ -d "$STORAGE_PATH" ]]; then
        FILE_COUNT=$(find "$STORAGE_PATH" -type f 2>/dev/null | wc -l)
        TOTAL_SIZE=$(du -sh "$STORAGE_PATH" 2>/dev/null | cut -f1)
        echo -e "  ${GREEN}✓${NC} Files: $FILE_COUNT ($TOTAL_SIZE)"
    else
        echo -e "  ${YELLOW}-${NC} No files yet"
    fi
    echo ""

    # Delta Tables
    echo -e "${YELLOW}Delta Tables:${NC}"
    DELTA_PATH="/tmp/vpipe_simulation/delta"
    if [[ -d "$DELTA_PATH" ]]; then
        TABLE_COUNT=0
        for table_dir in "$DELTA_PATH"/*; do
            if [[ -d "$table_dir/_delta_log" ]]; then
                table_name=$(basename "$table_dir")
                TABLE_COUNT=$((TABLE_COUNT + 1))
                echo -e "  ${GREEN}✓${NC} $table_name"
            fi
        done

        if [[ $TABLE_COUNT -eq 0 ]]; then
            echo -e "  ${YELLOW}-${NC} No tables yet"
        fi
    else
        echo -e "  ${YELLOW}-${NC} No Delta directory"
    fi
    echo ""

    # Logs
    echo -e "${YELLOW}Recent Errors (last 60s):${NC}"
    ERROR_COUNT=$(find /tmp -name "vpipe_*.log" -mmin -1 -exec grep -l ERROR {} \; 2>/dev/null | wc -l)
    if [[ $ERROR_COUNT -gt 0 ]]; then
        echo -e "  ${RED}✗${NC} Found errors in $ERROR_COUNT log files"
        find /tmp -name "vpipe_*.log" -mmin -1 -exec grep ERROR {} \; 2>/dev/null | tail -3 | sed 's/^/    /'
    else
        echo -e "  ${GREEN}✓${NC} No recent errors"
    fi
    echo ""

    echo -e "${BLUE}========================================${NC}"
    echo -e "Press Ctrl+C to exit | Refreshing in ${REFRESH_INTERVAL}s..."

    sleep "$REFRESH_INTERVAL"
done
