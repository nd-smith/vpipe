# CLAUDE.md

## Philosophy

Write simple, readable, maintainable Python. The human maintainer is not a senior developer—clarity beats cleverness every time. Code should be self-documenting; comments exist only when the code cannot speak for itself.


Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

Tradeoff: These guidelines bias toward caution over speed. For trivial tasks, use judgment.
1. Think Before Coding

Don't assume. Don't hide confusion. Surface tradeoffs.

Before implementing:

    State your assumptions explicitly. If uncertain, ask.
    If multiple interpretations exist, present them - don't pick silently.
    If a simpler approach exists, say so. Push back when warranted.
    If something is unclear, stop. Name what's confusing. Ask.

2. Simplicity First

Minimum code that solves the problem. Nothing speculative.

    No features beyond what was asked.
    No abstractions for single-use code.
    No "flexibility" or "configurability" that wasn't requested.
    No error handling for impossible scenarios.
    If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.
3. Surgical Changes

Touch only what you must. Clean up only your own mess.

When editing existing code:

    Don't "improve" adjacent code, comments, or formatting.
    Don't refactor things that aren't broken.
    Match existing style, even if you'd do it differently.
    If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

    Remove imports/variables/functions that YOUR changes made unused.
    Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.
4. Goal-Driven Execution

Define success criteria. Loop until verified.

Transform tasks into verifiable goals:

    "Add validation" → "Write tests for invalid inputs, then make them pass"
    "Fix the bug" → "Write a test that reproduces it, then make it pass"
    "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

######

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

