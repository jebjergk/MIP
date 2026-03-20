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
import json
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

_UX_GLOSSARY_PATH = (
    Path(__file__).resolve().parent
    .parent
    .parent
    / "mip_ui_web" / "src" / "data" / "UX_METRIC_GLOSSARY.json"
)

_cached_content: str | None = None
_cached_sections: list[dict[str, str]] | None = None

# Keep Ask MIP grounded only in active, non-deprecated guide sections.
_ACTIVE_GUIDE_FILES = [
    "01-big-picture.md",
    "02-daily-pipeline.md",
    "03-signals.md",
    "04-outcomes.md",
    "05-training-stages.md",
    "06-trust.md",
    "07-hit-rate.md",
    "08-avg-return.md",
    "09-trading.md",
    "10-patterns.md",
    "11-home.md",
    "12-cockpit.md",
    "15-training-status.md",
    "16-performance-dashboard.md",
    "17-symbol-tracker.md",
    "18-market-timeline.md",
    "19-runs.md",
    "20-debug.md",
    "21-parallel-worlds.md",
    "22-glossary.md",
    "26-news-intelligence.md",
    "27-live-portfolio-config.md",
    "28-ai-agent-decisions.md",
    "29-live-portfolio-activity.md",
    "30-learning-ledger.md",
    "31-ui-terms-and-labels.md",
]

_ACTIVE_UX_GLOSSARY_CATEGORIES = {
    "audit",
    "portfolio",
    "risk_gate",
    "signals",
    "proposals",
    "positions",
    "trades",
    "ui",
    "training_status",
    "performance",
    "brief",
    "home",
}


def _load_ux_glossary_markdown() -> str:
    """
    Convert selected UX metric glossary JSON categories into compact markdown.
    This gives Ask MIP broad term coverage across active app surfaces.
    """
    if not _UX_GLOSSARY_PATH.is_file():
        logger.warning("UX glossary file not found at %s", _UX_GLOSSARY_PATH)
        return ""

    try:
        raw = json.loads(_UX_GLOSSARY_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.error("Failed to parse UX glossary JSON: %s", exc)
        return ""

    lines: list[str] = [
        "# App-wide UI Terms (Active)",
        "",
        "Use these as supplementary definitions for page labels and metrics.",
        "",
    ]

    for category in sorted(_ACTIVE_UX_GLOSSARY_CATEGORIES):
        group = raw.get(category)
        if not isinstance(group, dict):
            continue
        lines.append(f"## {category.replace('_', ' ').title()}")
        lines.append("")
        for term_key, term_def in group.items():
            if not isinstance(term_def, dict):
                continue
            short = str(term_def.get("short") or "").strip()
            long = str(term_def.get("long") or "").strip()
            what = str(term_def.get("what") or "").strip()
            why = str(term_def.get("why") or "").strip()
            how = str(term_def.get("how") or "").strip()
            next_step = str(term_def.get("next") or "").strip()
            lines.append(f"### {term_key}")
            if short:
                lines.append(f"- Short: {short}")
            if long:
                lines.append(f"- Long: {long}")
            if what:
                lines.append(f"- What: {what}")
            if why:
                lines.append(f"- Why: {why}")
            if how:
                lines.append(f"- How: {how}")
            if next_step:
                lines.append(f"- Next: {next_step}")
            lines.append("")

    return "\n".join(lines).strip()


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
            "The MIP User Guide content is currently unavailable. "
            "Do not invent facts. Ask the user for context and direct them to verify in the UI."
        )
        return _cached_content

    md_files = [_GUIDE_DIR / name for name in _ACTIVE_GUIDE_FILES if (_GUIDE_DIR / name).is_file()]
    if not md_files:
        logger.warning("No active guide .md files found in %s", _GUIDE_DIR)
        _cached_content = "No guide sections found."
        return _cached_content

    sections = []
    section_rows: list[dict[str, str]] = []
    for f in md_files:
        try:
            text = f.read_text(encoding="utf-8")
            sections.append(text)
            title = f.stem
            for line in text.splitlines():
                stripped = line.strip()
                if stripped.startswith("# "):
                    title = stripped.replace("# ", "", 1).strip()
                    break
            section_rows.append(
                {
                    "file": f.name,
                    "title": title,
                    "content": text,
                }
            )
        except Exception as exc:
            logger.error("Failed to read %s: %s", f.name, exc)

    extra_glossary = _load_ux_glossary_markdown()
    if extra_glossary:
        sections.append(extra_glossary)

    _cached_content = "\n\n---\n\n".join(sections)
    global _cached_sections
    _cached_sections = section_rows
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
    global _cached_sections
    _cached_content = None
    _cached_sections = None
    return get_guide_content()


def get_guide_sections() -> list[dict[str, str]]:
    """Return active guide sections with filename/title/content metadata."""
    global _cached_sections
    if _cached_sections is None:
        get_guide_content()
    return _cached_sections or []
