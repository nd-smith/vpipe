#!/bin/bash
# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

# Cleanup script for simulation mode data
# Removes all temporary files, Delta tables, and logs created during simulation

set -e

SIMULATION_DIR="${SIMULATION_DIR:-/tmp/vpipe_simulation}"

echo "==================================================================="
echo "Simulation Data Cleanup"
echo "==================================================================="
echo "This script will remove all simulation data from: $SIMULATION_DIR"
echo ""

# Check if directory exists
if [ ! -d "$SIMULATION_DIR" ]; then
    echo "✅ No simulation data found at $SIMULATION_DIR"
    exit 0
fi

# Show what will be deleted
echo "The following will be deleted:"
echo ""

if [ -d "$SIMULATION_DIR/delta" ]; then
    echo "📊 Delta Lake tables:"
    for table in "$SIMULATION_DIR/delta"/*; do
        if [ -d "$table" ]; then
            table_name=$(basename "$table")
            row_count="unknown"

            # Try to count rows using Python if polars is available
            if command -v python3 &> /dev/null; then
                row_count=$(python3 -c "
try:
    import polars as pl
    df = pl.read_delta('$table')
    print(len(df))
except:
    print('unknown')
" 2>/dev/null || echo "unknown")
            fi

            echo "   - $table_name ($row_count rows)"
        fi
    done
    echo ""
fi

if [ -d "$SIMULATION_DIR/storage" ]; then
    echo "💾 Local storage files:"
    file_count=$(find "$SIMULATION_DIR/storage" -type f 2>/dev/null | wc -l)
    echo "   - $file_count files"
    echo ""
fi

# Calculate total size
total_size=$(du -sh "$SIMULATION_DIR" 2>/dev/null | cut -f1)
echo "📦 Total size: $total_size"
echo ""

# Confirm deletion
read -p "Proceed with cleanup? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cleanup cancelled"
    exit 0
fi

echo ""
echo "Cleaning up simulation data..."

# Clean Delta Lake tables
if [ -d "$SIMULATION_DIR/delta" ]; then
    echo "🗑️  Removing Delta Lake tables..."
    rm -rf "$SIMULATION_DIR/delta"/*
    echo "   ✅ Delta tables removed"
fi

# Clean storage files
if [ -d "$SIMULATION_DIR/storage" ]; then
    echo "🗑️  Removing storage files..."
    rm -rf "$SIMULATION_DIR/storage"/*
    echo "   ✅ Storage files removed"
fi

# Clean any other subdirectories
for dir in "$SIMULATION_DIR"/*; do
    if [ -d "$dir" ]; then
        dir_name=$(basename "$dir")
        if [ "$dir_name" != "delta" ] && [ "$dir_name" != "storage" ]; then
            echo "🗑️  Removing $dir_name..."
            rm -rf "$dir"
            echo "   ✅ $dir_name removed"
        fi
    fi
done

# Optionally remove the base directory
echo ""
read -p "Remove entire simulation directory? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -rf "$SIMULATION_DIR"
    echo "   ✅ Simulation directory removed: $SIMULATION_DIR"
else
    echo "   ℹ️  Base directory kept: $SIMULATION_DIR"
fi

echo ""
echo "==================================================================="
echo "✅ Cleanup complete"
echo "==================================================================="
