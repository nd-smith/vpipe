# CLAUDE.md

## Philosophy

Write simple, readable, maintainable Python. The human maintainer is not a senior developer—clarity beats cleverness every time. Code should be self-documenting; comments exist only when the code cannot speak for itself.

## Code Style

### General
- Simple and explicit over abstract and clever
- Flat is better than nested
- One obvious way to do things
- Follow Google's Python Style Guide where not otherwise specified

### Preferences
- Early returns over nested conditionals
- List comprehensions over map/filter (when readable)
- Context managers for resource handling
- `pathlib` over `os.path`
- f-strings over `.format()` or `%`
- Explicit `None` checks over truthiness when semantics matter

### Avoid
- Metaclasses, decorators with complex logic, or descriptors unless truly necessary
- Multiple inheritance
- `*args`/`**kwargs` unless building a genuine passthrough

### Naming
- Descriptive variable and function names (readability > brevity)
- Follow PEP 8 conventions
- Name functions as verbs, classes as nouns

### Comments
- The best comment is clean code that needs no comment
- Only comment *why*, never *what*
- No redundant docstrings (e.g., `"""Gets the user."""` on `get_user()`)
- Docstrings for public APIs and non-obvious behavior only

### Structure
- Small functions with single responsibilities
- Avoid premature abstraction—wait until a pattern repeats
- No unnecessary classes; functions are fine
- Keep imports organized: stdlib, third-party, local

## What Not To Do

- No unsolicited refactoring of surrounding code
- No "defensive" try/except blocks unless error handling was requested
- No adding logging, type hints, or validation beyond what's asked
- No helper functions unless genuinely reusable
- No comments that restate the code
- No placeholder or TODO comments unless requested

## Changes and Suggestions

- Implement what was asked for
- If you see a better approach or potential issue, note it briefly and ask—don't just do it
- Major deviations from the request require approval

## Testing

- Framework: pytest
- Do not add tests unless asked
- When working on non-trivial logic, ask if tests are wanted
- Test names should describe the scenario: `test_returns_none_when_user_not_found`

## Environment

- Python (corporate environment)
- CI/CD automated deployment
- Code must pass review before shipping
