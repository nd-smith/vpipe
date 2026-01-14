#!/usr/bin/env python3
"""Scan for potential missing awaits on async functions."""
import ast
import sys
from pathlib import Path
from typing import Set, List, Tuple


class AsyncCallVisitor(ast.NodeVisitor):
    """AST visitor to find async function calls without await."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.async_functions: Set[str] = set()
        self.issues: List[Tuple[int, str]] = []
        self.current_function = None
        self.in_async_context = False

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        """Track async function definitions."""
        # Store function name
        self.async_functions.add(node.name)

        # Enter async context
        old_context = self.in_async_context
        old_function = self.current_function
        self.in_async_context = True
        self.current_function = node.name

        self.generic_visit(node)

        # Restore context
        self.in_async_context = old_context
        self.current_function = old_function

    def visit_Call(self, node: ast.Call):
        """Check for async function calls without await."""
        # Only check calls inside async functions
        if not self.in_async_context:
            self.generic_visit(node)
            return

        # Check if this is a method call (self.method())
        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr

            # Check if parent is await expression
            # We need to track this in visit_Await instead
            # For now, we'll check common async patterns

            # Common async method patterns
            async_patterns = [
                'send', 'receive', 'process', 'execute', 'handle',
                'fetch', 'get', 'put', 'delete', 'update',
                'start', 'stop', 'close', 'connect', 'disconnect',
                'read', 'write', 'load', 'save', 'produce',
                'consume', 'commit', 'rollback', 'ensure',
                'record_enrichment', 'record_download', 'track_metric'
            ]

            if any(pattern in method_name for pattern in async_patterns):
                # Check if this call is directly awaited
                # We can't easily check this in visit_Call, so we mark it as suspicious
                pass

        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr):
        """Check for expression statements that might be unawaited async calls."""
        # Only check inside async functions
        if not self.in_async_context:
            self.generic_visit(node)
            return

        # Check if the expression is a Call (not awaited)
        if isinstance(node.value, ast.Call):
            call = node.value

            # Check for method calls
            if isinstance(call.func, ast.Attribute):
                method_name = call.func.attr

                # Check for common async method patterns
                async_patterns = [
                    'record_', 'track_', 'send', 'process',
                    'execute', 'handle', 'fetch', 'save',
                    'close', 'stop', 'commit'
                ]

                if any(method_name.startswith(pattern) for pattern in async_patterns):
                    # This is a suspicious call - method call without await
                    self.issues.append((
                        node.lineno,
                        f"Possible missing await: {method_name}(...) called without await"
                    ))

        self.generic_visit(node)


def scan_file(filepath: Path) -> List[Tuple[int, str]]:
    """Scan a Python file for missing awaits."""
    try:
        with open(filepath, 'r') as f:
            content = f.read()

        tree = ast.parse(content, filename=str(filepath))
        visitor = AsyncCallVisitor(str(filepath))
        visitor.visit(tree)

        return visitor.issues
    except SyntaxError as e:
        print(f"Syntax error in {filepath}: {e}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"Error scanning {filepath}: {e}", file=sys.stderr)
        return []


def scan_directory(directory: Path, pattern: str = "**/*.py") -> None:
    """Scan all Python files in a directory."""
    found_issues = False

    for filepath in sorted(directory.glob(pattern)):
        if filepath.is_file():
            issues = scan_file(filepath)
            if issues:
                found_issues = True
                print(f"\n{filepath}:")
                for lineno, message in issues:
                    print(f"  Line {lineno}: {message}")

    if not found_issues:
        print("No missing await issues found!")


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        path = Path(sys.argv[1])
    else:
        # Default to src directory
        path = Path(__file__).parent.parent / "src"

    if not path.exists():
        print(f"Error: Path {path} does not exist", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning for missing awaits in: {path}")
    print("=" * 80)

    if path.is_file():
        issues = scan_file(path)
        if issues:
            print(f"\n{path}:")
            for lineno, message in issues:
                print(f"  Line {lineno}: {message}")
        else:
            print("No issues found!")
    else:
        scan_directory(path)


if __name__ == "__main__":
    main()
