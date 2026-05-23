# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Extract CQL examples from documentation .rst and .md files.

This module parses documentation files and extracts CQL code blocks
that can be executed against a live ScyllaDB instance for validation.

It distinguishes between:
- Executable CQL (CREATE TABLE, INSERT, SELECT, etc.)
- Grammar definitions (contain backtick-quoted tokens like `name`)
- Output blocks (.. code-block:: none)
"""

import re
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class CqlBlock:
    """A single CQL code block extracted from documentation."""
    source_file: str
    line_number: int
    statements: list[str] = field(default_factory=list)
    # If True, this block is a grammar definition, not executable CQL
    is_grammar: bool = False


def is_grammar_definition(text: str) -> bool:
    """Heuristic: grammar definitions use backtick-quoted tokens like `name`."""
    # Count backtick-quoted tokens (RST inline literals)
    token_count = len(re.findall(r'`\w+`', text))
    # Grammar blocks tend to have ':' for BNF-like definitions
    has_bnf = bool(re.search(r'^\s*\w+\s*:', text, re.MULTILINE))
    # If there are many tokens and BNF patterns, it's grammar
    if token_count >= 2 and has_bnf:
        return True
    # Also check for re('...') patterns used in grammar
    if re.search(r"re\('[^']+'\)", text):
        return True
    return False


def is_executable_cql(statement: str) -> bool:
    """Check if a statement looks like executable CQL (not a fragment)."""
    s = statement.strip().upper()
    # Must start with a known CQL keyword
    cql_keywords = [
        'CREATE', 'DROP', 'ALTER', 'INSERT', 'UPDATE', 'DELETE',
        'SELECT', 'USE', 'GRANT', 'REVOKE', 'TRUNCATE', 'BEGIN',
        'DESCRIBE', 'LIST', 'APPLY',
    ]
    return any(s.startswith(kw) for kw in cql_keywords)


def clean_cql_text(text: str) -> str:
    """Remove cqlsh prompts, output tables, and other non-CQL noise."""
    lines = text.splitlines()
    cleaned = []
    for line in lines:
        # Strip cqlsh prompt prefix like "cqlsh:ks1> " or "cqlsh> "
        m = re.match(r'\s*cqlsh(?::\w+)?>\s*(.*)', line)
        if m:
            cleaned.append(m.group(1))
        # Skip output lines (table borders, result rows, "(N rows)" markers)
        elif re.match(r'\s*[-─┬┼┤├╭╰╮╯+|│]+\s*$', line):
            continue
        elif re.match(r'\s*\(\d+ rows?\)', line):
            continue
        # Skip lines that look like result data (start with spaces then values)
        elif re.match(r'\s+\S+\s*\|', line):
            continue
        # Skip column header lines (word | word | word)
        elif re.match(r'\s*\w+\s*\|', line) and not any(kw in line.upper() for kw in ['INSERT', 'SELECT', 'CREATE', 'UPDATE', 'DELETE', 'ALTER', 'DROP']):
            continue
        else:
            cleaned.append(line)
    return '\n'.join(cleaned)


def split_statements(text: str) -> list[str]:
    """Split a CQL text block into individual statements on semicolons.
    
    Handles semicolons inside string literals correctly.
    """
    statements = []
    current = []
    in_string = False
    quote_char = None

    for char in text:
        if in_string:
            current.append(char)
            if char == quote_char:
                in_string = False
        elif char in ("'", '"'):
            in_string = True
            quote_char = char
            current.append(char)
        elif char == ';':
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(char)

    # Handle last statement (may not end with ;)
    remainder = ''.join(current).strip()
    if remainder:
        statements.append(remainder)

    return statements


def extract_cql_blocks_rst(filepath: Path) -> list[CqlBlock]:
    """Extract CQL code blocks from an RST file."""
    blocks = []
    text_content = filepath.read_text()
    lines = text_content.splitlines()

    # Check if the file sets ".. highlight:: cql" — if so, bare code-block:: is also CQL
    has_highlight_cql = bool(re.search(r'^\.\.\s+highlight::\s*cql', text_content, re.MULTILINE))

    i = 0
    while i < len(lines):
        line = lines[i]
        # Match ".. code-block:: cql" or bare ".. code-block::" when highlight is cql
        # Also match lines ending with "::" (docutils literal block) when highlight is cql
        is_cql_block = re.match(r'\s*\.\.\s+code-block::\s*cql\s*$', line)
        is_bare_block = has_highlight_cql and re.match(r'\s*\.\.\s+code-block::\s*$', line)
        is_literal_block = has_highlight_cql and line.rstrip().endswith('::') and not line.strip().startswith('..')
        if is_cql_block or is_bare_block or is_literal_block:
            block_start = i + 1
            # Skip blank lines after directive
            i += 1
            while i < len(lines) and lines[i].strip() == '':
                i += 1
            # Collect indented content
            content_lines = []
            while i < len(lines):
                if lines[i].strip() == '' or lines[i].startswith(' ') or lines[i].startswith('\t'):
                    content_lines.append(lines[i])
                    i += 1
                else:
                    break
            text = '\n'.join(content_lines)
            # Remove common leading whitespace
            text = re.sub(r'^[ ]{3,4}', '', text, flags=re.MULTILINE)
            text = text.strip()

            if not text:
                continue

            block = CqlBlock(
                source_file=str(filepath),
                line_number=block_start,
                is_grammar=is_grammar_definition(text),
            )

            if not block.is_grammar:
                cleaned = clean_cql_text(text)
                block.statements = [s for s in split_statements(cleaned) if is_executable_cql(s)]

            blocks.append(block)
        else:
            i += 1

    return blocks


def extract_cql_blocks_md(filepath: Path) -> list[CqlBlock]:
    """Extract CQL code blocks from a Markdown file."""
    blocks = []
    text = filepath.read_text()
    # Match ```cql or ```sql blocks
    pattern = re.compile(r'^```(?:cql|sql)\s*\n(.*?)^```', re.MULTILINE | re.DOTALL)
    for match in pattern.finditer(text):
        content = match.group(1).strip()
        line_number = text[:match.start()].count('\n') + 1

        block = CqlBlock(
            source_file=str(filepath),
            line_number=line_number,
            is_grammar=is_grammar_definition(content),
        )
        if not block.is_grammar:
            block.statements = [s for s in split_statements(content) if is_executable_cql(s)]
        blocks.append(block)

    return blocks


def extract_cql_blocks(filepath: Path) -> list[CqlBlock]:
    """Extract CQL blocks from a documentation file (RST or MD)."""
    if filepath.suffix == '.rst':
        return extract_cql_blocks_rst(filepath)
    elif filepath.suffix == '.md':
        return extract_cql_blocks_md(filepath)
    return []


# Core CQL documentation files (most user-facing, highest priority)
CORE_DOC_FILES = [
    'docs/cql/ddl.rst',
    'docs/cql/dml/select.rst',
    'docs/cql/dml/insert.rst',
    'docs/cql/dml/update.rst',
    'docs/cql/dml/delete.rst',
    'docs/cql/dml/batch.rst',
    'docs/cql/time-to-live.rst',
    'docs/cql/json.rst',
    'docs/cql/types.rst',
    'docs/cql/functions.rst',
    'docs/cql/secondary-indexes.rst',
    'docs/cql/mv.rst',
]

FEATURE_DOC_FILES = [
    'docs/features/lwt.rst',
    'docs/features/counters.rst',
    'docs/features/materialized-views.rst',
    'docs/features/secondary-indexes.rst',
    'docs/features/local-secondary-indexes.rst',
]


if __name__ == '__main__':
    """Quick diagnostic: print extracted blocks from core docs."""
    import sys
    repo_root = Path(__file__).resolve().parent.parent.parent

    files = CORE_DOC_FILES if '--core' in sys.argv else CORE_DOC_FILES + FEATURE_DOC_FILES
    total_blocks = 0
    total_stmts = 0
    for rel_path in files:
        filepath = repo_root / rel_path
        if not filepath.exists():
            print(f"MISSING: {rel_path}")
            continue
        blocks = extract_cql_blocks(filepath)
        executable_blocks = [b for b in blocks if not b.is_grammar and b.statements]
        stmts = sum(len(b.statements) for b in executable_blocks)
        total_blocks += len(executable_blocks)
        total_stmts += stmts
        print(f"{rel_path}: {len(executable_blocks)} blocks, {stmts} statements")

    print(f"\nTotal: {total_blocks} executable blocks, {total_stmts} statements")
