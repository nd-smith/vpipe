#!/usr/bin/env python3
"""Verify simulation pipeline outputs.

Checks that simulation mode produced expected results:
- Files in local storage
- Delta tables with valid structure
- No critical errors

Usage:
    python scripts/verify_simulation.py
    python scripts/verify_simulation.py --domain claimx
"""

import argparse
import sys
from pathlib import Path
from typing import List, Tuple

# Color codes for terminal output
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'  # No Color


def verify_storage_files(simulation_dir: Path, domain: str) -> Tuple[bool, List[str]]:
    """Verify that files were uploaded to local storage.

    Args:
        simulation_dir: Base simulation directory
        domain: Domain to check (claimx or xact)

    Returns:
        Tuple of (success, messages)
    """
    messages = []
    storage_path = simulation_dir / domain

    if not storage_path.exists():
        messages.append(f"{YELLOW}⚠{NC}  Storage directory not found: {storage_path}")
        return True, messages  # Not an error if no files were generated

    file_count = len(list(storage_path.rglob("*")))
    if file_count == 0:
        messages.append(f"{YELLOW}⚠{NC}  No files in storage (may be expected)")
    else:
        messages.append(f"{GREEN}✓{NC} Storage: {file_count} files in {storage_path}")

        # List directories
        dirs = [d for d in storage_path.iterdir() if d.is_dir()]
        if dirs:
            messages.append(f"  Directories: {', '.join(d.name for d in dirs[:5])}")

    return True, messages


def verify_delta_tables(simulation_dir: Path, domain: str) -> Tuple[bool, List[str]]:
    """Verify that Delta tables were created.

    Args:
        simulation_dir: Base simulation directory
        domain: Domain to check (claimx or xact)

    Returns:
        Tuple of (success, messages)
    """
    messages = []
    delta_dir = simulation_dir / "delta"

    if not delta_dir.exists():
        messages.append(f"{RED}✗{NC} Delta directory not found: {delta_dir}")
        return False, messages

    messages.append(f"{GREEN}✓{NC} Delta directory exists: {delta_dir}")

    # Expected tables based on domain
    if domain == "claimx":
        expected_tables = [
            "claimx_projects",
            "claimx_contacts",
            "claimx_attachment_metadata",
            "claimx_tasks",
        ]
    else:
        expected_tables = [
            "xact_events",
            "xact_inventory",
        ]

    found_tables = []
    missing_tables = []

    for table_name in expected_tables:
        table_path = delta_dir / table_name
        delta_log = table_path / "_delta_log"

        if table_path.exists() and delta_log.exists():
            found_tables.append(table_name)
            messages.append(f"{GREEN}✓{NC} Delta table: {table_name}")

            # Count transaction logs
            log_files = list(delta_log.glob("*.json"))
            if log_files:
                messages.append(f"  Transaction logs: {len(log_files)}")
        else:
            missing_tables.append(table_name)

    # Check for unexpected tables
    all_tables = [d.name for d in delta_dir.iterdir() if d.is_dir()]
    unexpected = set(all_tables) - set(expected_tables)
    if unexpected:
        messages.append(f"{BLUE}ℹ{NC}  Unexpected tables: {', '.join(unexpected)}")

    # Determine success
    if len(found_tables) == 0:
        messages.append(f"{RED}✗{NC} No valid Delta tables found")
        return False, messages
    elif missing_tables:
        messages.append(f"{YELLOW}⚠{NC}  Missing tables: {', '.join(missing_tables)}")
        return True, messages  # Partial success
    else:
        messages.append(f"{GREEN}✓{NC} All expected Delta tables found ({len(found_tables)})")
        return True, messages


def verify_delta_table_contents(simulation_dir: Path, domain: str) -> Tuple[bool, List[str]]:
    """Verify Delta table contents using polars/deltalake.

    Args:
        simulation_dir: Base simulation directory
        domain: Domain to check

    Returns:
        Tuple of (success, messages)
    """
    messages = []

    try:
        import polars as pl
    except ImportError:
        messages.append(f"{YELLOW}⚠{NC}  Skipping content verification (polars not installed)")
        return True, messages

    delta_dir = simulation_dir / "delta"

    # Check a few key tables
    if domain == "claimx":
        tables_to_check = ["claimx_projects", "claimx_contacts"]
    else:
        tables_to_check = ["xact_events"]

    for table_name in tables_to_check:
        table_path = delta_dir / table_name

        if not table_path.exists():
            continue

        try:
            df = pl.read_delta(str(table_path))
            row_count = len(df)
            col_count = len(df.columns)

            if row_count > 0:
                messages.append(
                    f"{GREEN}✓{NC} {table_name}: {row_count} rows, {col_count} columns"
                )
            else:
                messages.append(f"{YELLOW}⚠{NC}  {table_name}: table exists but is empty")

        except Exception as e:
            messages.append(f"{RED}✗{NC} Failed to read {table_name}: {e}")
            return False, messages

    return True, messages


def verify_simulation(
    simulation_dir: Path = Path("/tmp/vpipe_simulation"),
    domain: str = "claimx",
    verbose: bool = False,
) -> bool:
    """Run all verification checks.

    Args:
        simulation_dir: Base simulation directory
        domain: Domain to verify
        verbose: Show detailed output

    Returns:
        True if all checks pass, False otherwise
    """
    print(f"{BLUE}========================================{NC}")
    print(f"{BLUE}Simulation Verification{NC}")
    print(f"{BLUE}========================================{NC}")
    print(f"Domain: {domain}")
    print(f"Path: {simulation_dir}")
    print()

    all_success = True
    all_messages = []

    # Run verifications
    checks = [
        ("Storage Files", verify_storage_files),
        ("Delta Tables", verify_delta_tables),
        ("Table Contents", verify_delta_table_contents),
    ]

    for check_name, check_func in checks:
        print(f"{YELLOW}{check_name}:{NC}")
        success, messages = check_func(simulation_dir, domain)

        for msg in messages:
            print(f"  {msg}")

        all_messages.extend(messages)
        if not success:
            all_success = False

        print()

    # Summary
    print(f"{BLUE}========================================{NC}")
    if all_success:
        print(f"{GREEN}✅ All verification checks passed!{NC}")
    else:
        print(f"{RED}❌ Some verification checks failed{NC}")
    print(f"{BLUE}========================================{NC}")

    return all_success


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Verify simulation pipeline outputs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Verify ClaimX simulation
    python scripts/verify_simulation.py

    # Verify XACT simulation
    python scripts/verify_simulation.py --domain xact

    # Custom simulation directory
    python scripts/verify_simulation.py --path /tmp/my_simulation
        """,
    )

    parser.add_argument(
        "--domain",
        choices=["claimx", "xact"],
        default="claimx",
        help="Domain to verify (default: claimx)",
    )

    parser.add_argument(
        "--path",
        type=Path,
        default=Path("/tmp/vpipe_simulation"),
        help="Simulation directory path (default: /tmp/vpipe_simulation)",
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show detailed output",
    )

    args = parser.parse_args()

    success = verify_simulation(
        simulation_dir=args.path,
        domain=args.domain,
        verbose=args.verbose,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
