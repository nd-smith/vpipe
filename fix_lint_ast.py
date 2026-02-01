#!/usr/bin/env python3
"""
AST-based script to fix logging lint errors:
- G004: Convert f-strings in logging to lazy % formatting
- G201: Convert .error(..., exc_info=True) to .exception(...)
"""
import ast
import sys
from pathlib import Path
from typing import List, Tuple


class LoggingFixer(ast.NodeTransformer):
    """AST transformer to fix logging issues."""

    def __init__(self):
        self.changes = []

    def visit_Call(self, node):
        # Continue visiting child nodes
        self.generic_visit(node)

        # Check if this is a logging call
        if isinstance(node.func, ast.Attribute):
            attr_name = node.func.attr

            # Check if it's a logging method
            if attr_name in ('debug', 'info', 'warning', 'error', 'critical', 'exception'):
                # Check if the object is 'logger' or 'self.logger'
                is_logger = False
                if isinstance(node.func.value, ast.Name) and node.func.value.id == 'logger':
                    is_logger = True
                elif isinstance(node.func.value, ast.Attribute) and node.func.value.attr == 'logger':
                    is_logger = True

                if is_logger and node.args:
                    # Check for f-string in first argument (G004)
                    first_arg = node.args[0]
                    if isinstance(first_arg, ast.JoinedStr):  # f-string
                        # Convert f-string to format string + args
                        self._fix_fstring_logging(node)

                    # Check for exc_info=True (G201)
                    if attr_name == 'error':
                        self._fix_exc_info(node)

        return node

    def _fix_fstring_logging(self, node):
        """Convert f-string logging to lazy % formatting."""
        # Note: This is complex to implement correctly with AST
        # For now, mark these for manual review
        pass

    def _fix_exc_info(self, node):
        """Convert .error(..., exc_info=True) to .exception(...)."""
        # Check keywords for exc_info=True
        for keyword in node.keywords:
            if keyword.arg == 'exc_info':
                if isinstance(keyword.value, ast.Constant) and keyword.value.value is True:
                    # Change method name from 'error' to 'exception'
                    node.func.attr = 'exception'
                    # Remove exc_info=True keyword
                    node.keywords.remove(keyword)
                    break


def fix_file_with_regex(content: str) -> str:
    """Use regex to fix simpler patterns that AST can't easily handle."""
    import re

    # Fix multi-line f-string logging calls
    # Pattern: logger.method(\n    f"string with {vars}"

    lines = content.split('\n')
    result_lines = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # Check if line has a logging call start
        if re.search(r'logger\.(debug|info|warning|error|critical|exception)\s*\(', line):
            # Look ahead for f-string on next line
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                fstring_match = re.match(r'(\s+)f"([^"]*)"', next_line)

                if fstring_match:
                    indent = fstring_match.group(1)
                    fstring_content = fstring_match.group(2)

                    # Extract {expressions} from f-string
                    expr_pattern = r'\{([^}]+?)\}'
                    expressions = re.findall(expr_pattern, fstring_content)

                    if expressions:
                        # Replace {expr} with %s
                        format_str = re.sub(expr_pattern, '%s', fstring_content)

                        # Rebuild the line
                        result_lines.append(line)

                        # Check if there's a trailing comma
                        trailing = ','
                        if i + 2 < len(lines) and lines[i + 2].strip().startswith('extra='):
                            trailing = ','
                        elif i + 2 < len(lines) and lines[i + 2].strip() == ')':
                            trailing = ''

                        # Add format string line
                        result_lines.append(f'{indent}"{format_str}",')

                        # Add arguments
                        args_line = f'{indent}{", ".join(expressions)}{trailing}'
                        result_lines.append(args_line)

                        # Skip the original f-string line
                        i += 2
                        continue

        result_lines.append(line)
        i += 1

    return '\n'.join(result_lines)


def fix_exc_info_with_regex(content: str) -> str:
    """Fix .error(..., exc_info=True) patterns with regex."""
    import re

    # Pattern 1: logger.error(..., exc_info=True)
    content = re.sub(
        r'logger\.error\(([^)]*),\s*exc_info=True\s*\)',
        r'logger.exception(\1)',
        content,
        flags=re.DOTALL
    )

    # Pattern 2: self.logger.error(..., exc_info=True)
    content = re.sub(
        r'self\.logger\.error\(([^)]*),\s*exc_info=True\s*\)',
        r'self.logger.exception(\1)',
        content,
        flags=re.DOTALL
    )

    return content


def process_file(file_path: Path) -> bool:
    """Process a file and return True if changes were made."""
    try:
        content = file_path.read_text()
        original = content

        # Apply regex-based fixes
        content = fix_file_with_regex(content)
        content = fix_exc_info_with_regex(content)

        if content != original:
            file_path.write_text(content)
            return True
        return False
    except Exception as e:
        print(f"Error processing {file_path}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
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
