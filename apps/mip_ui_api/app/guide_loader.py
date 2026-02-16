"""
Guide content loader — reads all User Guide markdown files once and caches
the concatenated result for use as the system-prompt knowledge base in the
Ask MIP endpoint.

The markdown source of truth lives in the frontend repo:
  MIP/apps/mip_ui_web/src/guide/*.md

This module resolves the path relative to its own location so it works
regardless of the working directory used to launch the API.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Resolve guide directory relative to this file:
# this file → app/ → mip_ui_api/ → apps/ → (up to mip_ui_web) src/guide/
_GUIDE_DIR = (
    Path(__file__).resolve().parent    # app/
    .parent                            # mip_ui_api/
    .parent                            # apps/
    / "mip_ui_web" / "src" / "guide"
)

_cached_content: str | None = None


def get_guide_content() -> str:
    """
    Return the full User Guide as a single markdown string.
    Reads from disk on first call, then returns cached copy.
    """
    global _cached_content
    if _cached_content is not None:
        return _cached_content

    if not _GUIDE_DIR.is_dir():
        logger.warning("Guide directory not found at %s — using fallback.", _GUIDE_DIR)
        _cached_content = (
            "The MIP User Guide content is not available. "
            "Answer based on your general knowledge of MIP as described in this conversation."
        )
        return _cached_content

    md_files = sorted(_GUIDE_DIR.glob("*.md"))
    if not md_files:
        logger.warning("No .md files found in %s", _GUIDE_DIR)
        _cached_content = "No guide sections found."
        return _cached_content

    sections = []
    for f in md_files:
        try:
            sections.append(f.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error("Failed to read %s: %s", f.name, exc)

    _cached_content = "\n\n---\n\n".join(sections)
    logger.info(
        "Loaded %d guide sections (%d chars) from %s",
        len(sections),
        len(_cached_content),
        _GUIDE_DIR,
    )
    return _cached_content


def reload_guide_content() -> str:
    """Force re-read from disk (useful after guide edits during development)."""
    global _cached_content
    _cached_content = None
    return get_guide_content()
