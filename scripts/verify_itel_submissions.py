#!/usr/bin/env python3
"""Verify iTel Cabinet submissions in simulation mode.

This script checks that iTel Cabinet API worker correctly writes submissions
to local files when running in simulation mode.

Usage:
    python scripts/verify_itel_submissions.py
    python scripts/verify_itel_submissions.py --verbose
    python scripts/verify_itel_submissions.py --output-dir /custom/path
"""

import argparse
import json
import sys
from pathlib import Path
from typing import List, Dict, Any


def verify_itel_submissions(output_dir: Path, verbose: bool = False) -> bool:
    """Check iTel submission files.

    Args:
        output_dir: Directory containing iTel submissions
        verbose: Print detailed information

    Returns:
        True if verification passed, False otherwise
    """
    print(f"\nVerifying iTel Cabinet submissions in: {output_dir}")
    print("=" * 80)

    # Check if directory exists
    if not output_dir.exists():
        print(f"❌ No iTel submissions directory found at: {output_dir}")
        print("\nExpected directory structure:")
        print("  /tmp/vpipe_simulation/")
        print("    └── itel_submissions/")
        print("          └── itel_submission_*.json")
        return False

    # Find all submission files
    submissions = list(output_dir.glob("itel_submission_*.json"))

    if not submissions:
        print(f"⚠️  No submissions found in {output_dir}")
        print("\nTo generate submissions, run:")
        print("  SIMULATION_MODE=true python -m kafka_pipeline dummy-source \\")
        print("    --domains claimx \\")
        print("    --plugin-profile itel_cabinet_api \\")
        print("    --max-events 10")
        return False

    print(f"✓ Found {len(submissions)} iTel submission(s)\n")

    # Verify each submission
    valid_count = 0
    error_count = 0

    for filepath in sorted(submissions):
        try:
            with open(filepath) as f:
                data = json.load(f)

            # Validate required fields
            errors = validate_submission(data, verbose)

            if errors:
                print(f"❌ {filepath.name}")
                for error in errors:
                    print(f"   {error}")
                error_count += 1
            else:
                if verbose:
                    print(f"✓ {filepath.name}")
                    print_submission_details(data)
                else:
                    print(f"✓ {filepath.name}")
                valid_count += 1

        except json.JSONDecodeError as e:
            print(f"❌ {filepath.name}")
            print(f"   Invalid JSON: {e}")
            error_count += 1
        except Exception as e:
            print(f"❌ {filepath.name}")
            print(f"   Error reading file: {e}")
            error_count += 1

    # Summary
    print("\n" + "=" * 80)
    print(f"Verification Summary:")
    print(f"  Total submissions: {len(submissions)}")
    print(f"  Valid: {valid_count}")
    print(f"  Errors: {error_count}")

    if error_count == 0:
        print("\n✓ All submissions are valid!")
        return True
    else:
        print(f"\n❌ Found {error_count} invalid submission(s)")
        return False


def validate_submission(data: Dict[str, Any], verbose: bool) -> List[str]:
    """Validate submission data structure.

    Args:
        data: Submission data to validate
        verbose: Print warnings for optional fields

    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    # Required top-level fields
    required_fields = [
        'assignment_id',
        'simulation_mode',
        'submitted_at',
    ]

    for field in required_fields:
        if field not in data:
            errors.append(f"Missing required field: {field}")

    # Check simulation_mode flag
    if data.get('simulation_mode') is not True:
        errors.append(f"simulation_mode must be true, got: {data.get('simulation_mode')}")

    # Check meta section
    meta = data.get('meta', {})
    if not meta and verbose:
        errors.append("Warning: meta section is empty")

    # Check customer section
    customer = data.get('customer', {})
    if not customer and verbose:
        errors.append("Warning: customer section is empty")

    # Check data section (organized by topics)
    topics_data = data.get('data', {})
    if not topics_data and verbose:
        errors.append("Warning: data section is empty")

    return errors


def print_submission_details(data: Dict[str, Any]):
    """Print detailed submission information.

    Args:
        data: Submission data to print
    """
    meta = data.get('meta', {})
    customer = data.get('customer', {})
    topics_data = data.get('data', {})

    print(f"   Assignment ID: {data.get('assignment_id')}")
    print(f"   Project ID: {meta.get('projectId')}")
    print(f"   Task ID: {meta.get('taskId')}")

    # Customer info
    if customer:
        first_name = customer.get('firstName', 'N/A')
        last_name = customer.get('lastName', 'N/A')
        print(f"   Customer: {first_name} {last_name}")

    # Cabinet damage data
    if topics_data:
        print(f"   Data Topics: {', '.join(topics_data.keys())}")

        # Check for cabinet damage details
        for topic_name, topic_data in topics_data.items():
            if 'Lower Cabinets' in topic_name:
                lower_lf = topic_data.get('lower_cabinets_lf', {}).get('answer', 'N/A')
                print(f"   Lower Cabinets: {lower_lf} LF")
            if 'Upper Cabinets' in topic_name:
                upper_lf = topic_data.get('upper_cabinets_lf', {}).get('answer', 'N/A')
                print(f"   Upper Cabinets: {upper_lf} LF")

    print(f"   Submitted: {data.get('submitted_at')}")
    print()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Verify iTel Cabinet submissions in simulation mode"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/vpipe_simulation/itel_submissions"),
        help="Directory containing iTel submissions (default: /tmp/vpipe_simulation/itel_submissions)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print detailed information about each submission"
    )

    args = parser.parse_args()

    # Run verification
    success = verify_itel_submissions(args.output_dir, args.verbose)

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
