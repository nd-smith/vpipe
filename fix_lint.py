#!/usr/bin/env python3
"""
Script to automatically fix common ruff lint errors:
- G004: Convert f-strings in logging to lazy % formatting
- G201: Convert .error(..., exc_info=True) to .exception(...)
"""
import re
import sys
from pathlib import Path


def fix_logging_fstrings(content: str) -> str:
    """Convert f-strings in logging calls to lazy % formatting."""
    # Pattern to match logging calls with f-strings
    # logger.info(f"text {var1} more {var2}")
    pattern = r'(logger\.(debug|info|warning|error|critical|exception))\(f"([^"]*?)"'

    def replace_fstring(match):
        method = match.group(1)
        log_level = match.group(2)
        fstring_content = match.group(3)

        # Find all {expressions} in the f-string
        expr_pattern = r'\{([^}]+?)\}'
        expressions = re.findall(expr_pattern, fstring_content)

        # Replace {expr} with %s in the format string
        format_str = re.sub(expr_pattern, '%s', fstring_content)

        # Build the new logging call
        if expressions:
            # Join expressions with commas
            args = ', '.join(expressions)
            return f'{method}("{format_str}", {args}'
        else:
            # No expressions, just remove the f prefix
            return f'{method}("{format_str}"'

    content = re.sub(pattern, replace_fstring, content)
    return content


def fix_error_exc_info(content: str) -> str:
    """Convert .error(..., exc_info=True) to .exception(...)."""
    # Pattern: logger.error(msg, ..., exc_info=True)
    # Replace with: logger.exception(msg, ...)

    # Multi-line pattern for error calls with exc_info=True
    pattern = r'logger\.error\((.*?),\s*exc_info=True\)'
    content = re.sub(pattern, r'logger.exception(\1)', content, flags=re.DOTALL)

    # Also handle self.logger
    pattern = r'self\.logger\.error\((.*?),\s*exc_info=True\)'
    content = re.sub(pattern, r'self.logger.exception(\1)', content, flags=re.DOTALL)

    return content


def fix_exception_chaining(content: str) -> str:
    """Add 'from None' to raise statements in except clauses where appropriate."""
    # Pattern: except ...: raise SomeError(...)
    # This is trickier and might need manual review, so we'll skip for now
    return content


def process_file(file_path: Path) -> bool:
    """Process a single file and return True if changes were made."""
    try:
        content = file_path.read_text()
        original = content

        # Apply fixes
        content = fix_logging_fstrings(content)
        content = fix_error_exc_info(content)

        if content != original:
            file_path.write_text(content)
            return True
        return False
    except Exception as e:
        print(f"Error processing {file_path}: {e}", file=sys.stderr)
        return False


def main():
    src_dir = Path(__file__).parent / "src"

    if not src_dir.exists():
        print(f"Error: {src_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    # Find all Python files
    py_files = list(src_dir.rglob("*.py"))

    print(f"Processing {len(py_files)} Python files...")
    modified_count = 0

    for py_file in py_files:
        if process_file(py_file):
            modified_count += 1
            print(f"Modified: {py_file.relative_to(src_dir.parent)}")

    print(f"\nModified {modified_count} files")


if __name__ == "__main__":
    main()
