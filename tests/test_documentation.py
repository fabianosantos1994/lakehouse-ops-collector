from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote

import pytest


ROOT = Path(__file__).resolve().parents[1]
IGNORED_DIRS = {".git", ".venv", ".pytest_cache", "__pycache__", "examples"}
LINK_PATTERN = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
HEADING_PATTERN = re.compile(r"^#{1,6}\s+(.+?)\s*$", re.MULTILINE)
FENCE_PATTERN = re.compile(r"```.*?```", re.DOTALL)


def _markdown_files() -> list[Path]:
    return sorted(
        path
        for path in ROOT.rglob("*.md")
        if not IGNORED_DIRS.intersection(path.relative_to(ROOT).parts)
    )


def _heading_anchors(path: Path) -> set[str]:
    """Gera as âncoras no mesmo formato usado pelo GitHub."""
    content = FENCE_PATTERN.sub("", path.read_text(encoding="utf-8"))
    return {
        re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")
        for heading in HEADING_PATTERN.findall(content)
    }


def _relative_links() -> list[tuple[Path, str]]:
    links = []
    for document in _markdown_files():
        content = FENCE_PATTERN.sub("", document.read_text(encoding="utf-8"))
        for target in LINK_PATTERN.findall(content):
            # Links externos e e-mails não são verificados.
            if not re.match(r"^[a-z]+:", target):
                links.append((document, target))
    return links


@pytest.mark.parametrize(
    ("document", "target"),
    _relative_links(),
    ids=lambda value: str(value.relative_to(ROOT)) if isinstance(value, Path) else value,
)
def test_relative_links_point_to_existing_files_and_anchors(document: Path, target: str):
    path_part, _, anchor = unquote(target).partition("#")
    resolved = (document.parent / path_part).resolve() if path_part else document

    assert resolved.exists(), f"arquivo inexistente: {resolved}"
    if anchor and resolved.suffix == ".md":
        assert anchor in _heading_anchors(resolved)
