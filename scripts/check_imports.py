#!/usr/bin/env python3
"""Check that lakehouse_core has no prohibited dependencies.

This script enforces architecture boundaries by checking for:
1. No Airflow imports in lakehouse_core/
2. No dags.* imports in lakehouse_core/

Usage:
    python scripts/check_imports.py [files...]

Exit codes:
    0: No violations found
    1: Violations found
"""

import re
import sys
from pathlib import Path


PROHIBITED_PATTERNS = [
    (r"from\s+airflow", "Airflow import"),
    (r"import\s+airflow", "Airflow import"),
    (r"from\s+dags\.", "dags.* import"),
    (r"import\s+dags\.", "dags.* import"),
]


def check_file(file_path: Path) -> list[tuple[int, str, str]]:
    """Check a single file for prohibited imports.

    Args:
        file_path: Path to Python file to check

    Returns:
        List of (line_number, line_content, violation_type) tuples
    """
    violations = []

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                line_stripped = line.strip()

                # Skip comments and empty lines
                if not line_stripped or line_stripped.startswith("#"):
                    continue

                # Check each prohibited pattern
                for pattern, violation_type in PROHIBITED_PATTERNS:
                    if re.search(pattern, line):
                        violations.append((line_num, line.strip(), violation_type))

    except Exception as e:
        print(f"Error reading {file_path}: {e}", file=sys.stderr)

    return violations


def main(file_paths: list[str]) -> int:
    """Check multiple files for prohibited imports.

    Args:
        file_paths: List of file paths to check

    Returns:
        Exit code (0 = success, 1 = violations found)
    """
    total_violations = 0

    for file_path_str in file_paths:
        file_path = Path(file_path_str)

        # Only check Python files in lakehouse_core/
        if not file_path.match("lakehouse_core/**/*.py"):
            continue

        violations = check_file(file_path)

        if violations:
            total_violations += len(violations)
            print(f"\n❌ {file_path}:")
            for line_num, line_content, violation_type in violations:
                print(f"  Line {line_num}: {violation_type}")
                print(f"    {line_content}")

    if total_violations > 0:
        print(f"\n❌ Found {total_violations} architecture violation(s)")
        print("\nℹ️  lakehouse_core/ must remain orchestrator-agnostic.")
        print("   Move orchestrator-specific code to dags/adapters/")
        print("   See lakehouse_core/ARCHITECTURE.md for details.")
        return 1
    else:
        print("✅ No architecture violations found")
        return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/check_imports.py [files...]")
        sys.exit(1)

    exit_code = main(sys.argv[1:])
    sys.exit(exit_code)
