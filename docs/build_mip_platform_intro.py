"""
Build MIP Platform Introduction Presentation
Market Research Platform + Broker-Linked Trading

Uses the MIP design system: dark headers, blue accents, white cards, edge bars,
process flows, chips, and visual analogies throughout.
"""

from pathlib import Path

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.util import Inches, Pt, Emu

# ── MIP palette ──────────────────────────────────────────────────────────────
MIP_BLUE      = RGBColor(48, 105, 240)
MIP_DARK      = RGBColor(15, 23, 42)
MIP_SLATE     = RGBColor(51, 65, 85)
MIP_BG        = RGBColor(245, 248, 252)
MIP_TEXT      = RGBColor(31, 41, 55)
MIP_MUTED     = RGBColor(95, 107, 122)
MIP_GREEN     = RGBColor(46, 125, 50)
MIP_AMBER     = RGBColor(146, 95, 0)
MIP_RED       = RGBColor(198, 40, 40)
MIP_PURPLE    = RGBColor(106, 27, 154)
MIP_TEAL      = RGBColor(0, 121, 107)
MIP_CYAN      = RGBColor(0, 151, 167)
MIP_INDIGO    = RGBColor(57, 73, 171)
WHITE         = RGBColor(255, 255, 255)
LIGHT_GRAY    = RGBColor(215, 220, 228)
CARD_BG       = RGBColor(255, 255, 255)
DARK_CARD_BG  = RGBColor(15, 23, 42)
DARK_CARD_BDR = RGBColor(36, 48, 65)
HERO_GREEN_BG = RGBColor(230, 245, 234)
HERO_BLUE_BG  = RGBColor(227, 236, 253)
HERO_PURP_BG  = RGBColor(243, 229, 245)
HERO_AMB_BG   = RGBColor(255, 248, 225)
FOOTER_BG     = RGBColor(238, 242, 248)
FOOTER_TEXT    = RGBColor(110, 123, 141)

SLIDE_W = Inches(13.333)
SLIDE_H = Inches(7.5)


# ── Helper functions ─────────────────────────────────────────────────────────

def _set_bg(slide, color: RGBColor) -> None:
    fill = slide.background.fill
    fill.solid()
    fill.fore_color.rgb = color


def _draw_header(slide, title: str, subtitle: str, section: str = "MIP PLATFORM") -> None:
    top = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0), Inches(0), SLIDE_W, Inches(0.78))
    top.fill.solid()
    top.fill.fore_color.rgb = MIP_DARK
    top.line.fill.background()

    accent = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0), Inches(0.75), SLIDE_W, Inches(0.06))
    accent.fill.solid()
    accent.fill.fore_color.rgb = MIP_BLUE
    accent.line.fill.background()

    sec_box = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, Inches(10.8), Inches(0.14), Inches(2.3), Inches(0.42)
    )
    sec_box.fill.solid()
    sec_box.fill.fore_color.rgb = RGBColor(36, 48, 71)
    sec_box.line.color.rgb = MIP_BLUE
    tf = sec_box.text_frame
    tf.text = section
    tf.paragraphs[0].alignment = PP_ALIGN.CENTER
    tf.paragraphs[0].font.size = Pt(11)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = RGBColor(225, 233, 246)

    t_box = slide.shapes.add_textbox(Inches(0.7), Inches(1.02), Inches(9.5), Inches(0.7))
    tf = t_box.text_frame
    tf.text = title
    tf.paragraphs[0].font.size = Pt(34)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = MIP_DARK

    s_box = slide.shapes.add_textbox(Inches(0.7), Inches(1.72), Inches(11.8), Inches(0.55))
    tf = s_box.text_frame
    tf.text = subtitle
    tf.paragraphs[0].font.size = Pt(16)
    tf.paragraphs[0].font.color.rgb = MIP_MUTED


def _add_card(
    slide, x: float, y: float, w: float, h: float,
    title: str, items: list[str], edge: RGBColor = MIP_BLUE,
    title_size: int = 15, item_size: int = 12,
) -> None:
    card = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h)
    )
    card.fill.solid()
    card.fill.fore_color.rgb = CARD_BG
    card.line.color.rgb = LIGHT_GRAY
    card.shadow.inherit = False

    bar = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, Inches(x), Inches(y), Inches(0.08), Inches(h)
    )
    bar.fill.solid()
    bar.fill.fore_color.rgb = edge
    bar.line.fill.background()

    tf = card.text_frame
    tf.word_wrap = True
    tf.clear()
    p0 = tf.paragraphs[0]
    p0.text = title
    p0.font.bold = True
    p0.font.size = Pt(title_size)
    p0.font.color.rgb = MIP_SLATE
    for item in items:
        p = tf.add_paragraph()
        p.text = f"  {item}"
        p.font.size = Pt(item_size)
        p.font.color.rgb = MIP_TEXT
        p.space_before = Pt(4)


def _add_dark_card(
    slide, x: float, y: float, w: float, h: float,
    title: str, items: list[str], edge: RGBColor = MIP_BLUE,
) -> None:
    card = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h)
    )
    card.fill.solid()
    card.fill.fore_color.rgb = DARK_CARD_BG
    card.line.color.rgb = DARK_CARD_BDR

    bar = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, Inches(x), Inches(y), Inches(0.08), Inches(h)
    )
    bar.fill.solid()
    bar.fill.fore_color.rgb = edge
    bar.line.fill.background()

    tf = card.text_frame
    tf.word_wrap = True
    tf.clear()
    p0 = tf.paragraphs[0]
    p0.text = title
    p0.font.bold = True
    p0.font.size = Pt(15)
    p0.font.color.rgb = RGBColor(226, 232, 240)
    for item in items:
        p = tf.add_paragraph()
        p.text = f"  {item}"
        p.font.size = Pt(12)
        p.font.color.rgb = RGBColor(203, 213, 225)
        p.space_before = Pt(3)


def _add_chip(
    slide, x: float, y: float, w: float, text: str,
    bg: RGBColor, fg: RGBColor = WHITE, h: float = 0.34,
) -> None:
    chip = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h)
    )
    chip.fill.solid()
    chip.fill.fore_color.rgb = bg
    chip.line.fill.background()
    tf = chip.text_frame
    tf.text = text
    tf.paragraphs[0].alignment = PP_ALIGN.CENTER
    tf.paragraphs[0].font.size = Pt(11)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = fg


def _add_process(slide, labels: list[str], y: float, start_x: float = 0.75, box_w: float = 1.88, gap: float = 0.22) -> None:
    for idx, label in enumerate(labels):
        x = start_x + idx * (box_w + gap)
        box = slide.shapes.add_shape(
            MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(box_w), Inches(0.85)
        )
        box.fill.solid()
        box.fill.fore_color.rgb = CARD_BG
        box.line.color.rgb = RGBColor(207, 214, 225)
        tf = box.text_frame
        tf.text = label
        tf.paragraphs[0].alignment = PP_ALIGN.CENTER
        tf.paragraphs[0].font.size = Pt(12)
        tf.paragraphs[0].font.bold = True
        tf.paragraphs[0].font.color.rgb = MIP_SLATE
        if idx < len(labels) - 1:
            arrow = slide.shapes.add_shape(
                MSO_SHAPE.CHEVRON,
                Inches(x + box_w + 0.02), Inches(y + 0.2),
                Inches(0.18), Inches(0.45),
            )
            arrow.fill.solid()
            arrow.fill.fore_color.rgb = MIP_BLUE
            arrow.line.fill.background()


def _add_icon_circle(
    slide, x: float, y: float, size: float,
    text: str, bg: RGBColor, fg: RGBColor = WHITE,
    font_size: int = 22,
) -> None:
    circle = slide.shapes.add_shape(
        MSO_SHAPE.OVAL, Inches(x), Inches(y), Inches(size), Inches(size)
    )
    circle.fill.solid()
    circle.fill.fore_color.rgb = bg
    circle.line.fill.background()
    tf = circle.text_frame
    tf.text = text
    tf.paragraphs[0].alignment = PP_ALIGN.CENTER
    tf.paragraphs[0].font.size = Pt(font_size)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = fg


def _add_hero_box(
    slide, x: float, y: float, w: float, h: float,
    bg: RGBColor, title: str, body_lines: list[str],
    title_color: RGBColor = MIP_DARK,
) -> None:
    box = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h)
    )
    box.fill.solid()
    box.fill.fore_color.rgb = bg
    box.line.color.rgb = LIGHT_GRAY
    tf = box.text_frame
    tf.word_wrap = True
    tf.text = title
    tf.paragraphs[0].font.size = Pt(18)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = title_color
    for line in body_lines:
        p = tf.add_paragraph()
        p.text = line
        p.font.size = Pt(13)
        p.font.color.rgb = MIP_TEXT
        p.space_before = Pt(4)


def _add_analogy_bar(slide, icon: str, text: str, y: float = 6.6) -> None:
    bar = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.7), Inches(y), Inches(11.9), Inches(0.5)
    )
    bar.fill.solid()
    bar.fill.fore_color.rgb = RGBColor(240, 243, 250)
    bar.line.color.rgb = LIGHT_GRAY
    tf = bar.text_frame
    tf.text = f"{icon}  {text}"
    tf.paragraphs[0].font.size = Pt(13)
    tf.paragraphs[0].font.italic = True
    tf.paragraphs[0].font.color.rgb = MIP_SLATE


def _add_footer(slide, text: str = "MIP  |  Market Intelligence Platform  |  Evidence  >  Decision  >  Execution") -> None:
    bar = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, Inches(0), Inches(7.18), SLIDE_W, Inches(0.32)
    )
    bar.fill.solid()
    bar.fill.fore_color.rgb = FOOTER_BG
    bar.line.fill.background()
    t = slide.shapes.add_textbox(Inches(0.6), Inches(7.22), Inches(10.0), Inches(0.2))
    tf = t.text_frame
    tf.text = text
    tf.paragraphs[0].font.size = Pt(10)
    tf.paragraphs[0].font.color.rgb = FOOTER_TEXT


def _add_stat_box(
    slide, x: float, y: float, w: float, h: float,
    value: str, label: str, color: RGBColor,
) -> None:
    box = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h)
    )
    box.fill.solid()
    box.fill.fore_color.rgb = CARD_BG
    box.line.color.rgb = LIGHT_GRAY
    tf = box.text_frame
    tf.word_wrap = True
    tf.text = value
    tf.paragraphs[0].alignment = PP_ALIGN.CENTER
    tf.paragraphs[0].font.size = Pt(28)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = color
    p = tf.add_paragraph()
    p.text = label
    p.alignment = PP_ALIGN.CENTER
    p.font.size = Pt(11)
    p.font.color.rgb = MIP_MUTED
    p.space_before = Pt(2)


def _add_gradient_banner(slide, x, y, w, h, color1, color2):
    """Simulate gradient with two overlapping rectangles."""
    r1 = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(x), Inches(y), Inches(w/2), Inches(h))
    r1.fill.solid()
    r1.fill.fore_color.rgb = color1
    r1.line.fill.background()
    r2 = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(x + w/2), Inches(y), Inches(w/2), Inches(h))
    r2.fill.solid()
    r2.fill.fore_color.rgb = color2
    r2.line.fill.background()


# ══════════════════════════════════════════════════════════════════════════════
#  SLIDE BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def _slide_title(prs: Presentation) -> None:
    """Slide 1: Grand opening — What is MIP."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_DARK)

    _add_gradient_banner(slide, 0, 0, 13.333, 7.5, MIP_DARK, RGBColor(20, 30, 58))

    accent = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, Inches(0.9), Inches(1.0), Inches(0.15), Inches(5.2)
    )
    accent.fill.solid()
    accent.fill.fore_color.rgb = MIP_BLUE
    accent.line.fill.background()

    title = slide.shapes.add_textbox(Inches(1.3), Inches(1.2), Inches(7.5), Inches(1.0))
    tf = title.text_frame
    tf.text = "Market Intelligence Platform"
    tf.paragraphs[0].font.size = Pt(44)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = WHITE

    sub1 = slide.shapes.add_textbox(Inches(1.3), Inches(2.3), Inches(8.0), Inches(0.6))
    tf = sub1.text_frame
    tf.text = "Where Market Research Meets Intelligent Execution"
    tf.paragraphs[0].font.size = Pt(22)
    tf.paragraphs[0].font.color.rgb = RGBColor(148, 163, 184)

    divider = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, Inches(1.3), Inches(3.1), Inches(3.0), Inches(0.04)
    )
    divider.fill.solid()
    divider.fill.fore_color.rgb = MIP_BLUE
    divider.line.fill.background()

    desc = slide.shapes.add_textbox(Inches(1.3), Inches(3.4), Inches(6.5), Inches(2.5))
    tf = desc.text_frame
    tf.word_wrap = True
    lines = [
        "MIP is a Snowflake-native platform that fuses AI-driven",
        "market research with live broker-linked trading.",
        "",
        "Every trade is backed by evidence. Every decision is auditable.",
        "Every outcome feeds the next cycle of learning.",
    ]
    tf.text = lines[0]
    tf.paragraphs[0].font.size = Pt(16)
    tf.paragraphs[0].font.color.rgb = RGBColor(203, 213, 225)
    for line in lines[1:]:
        p = tf.add_paragraph()
        p.text = line
        p.font.size = Pt(16)
        p.font.color.rgb = RGBColor(203, 213, 225)
        p.space_before = Pt(4)

    pillars = [
        ("RESEARCH", MIP_BLUE),
        ("DECISIONS", MIP_PURPLE),
        ("EXECUTION", MIP_GREEN),
        ("LEARNING", MIP_TEAL),
    ]
    for i, (label, color) in enumerate(pillars):
        _add_chip(slide, 1.3 + i * 2.2, 6.1, 1.9, label, color)

    right_card = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, Inches(8.3), Inches(1.5), Inches(4.5), Inches(4.8)
    )
    right_card.fill.solid()
    right_card.fill.fore_color.rgb = RGBColor(20, 32, 55)
    right_card.line.color.rgb = RGBColor(40, 58, 90)
    tf = right_card.text_frame
    tf.word_wrap = True
    tf.text = "Platform Highlights"
    tf.paragraphs[0].font.size = Pt(18)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = RGBColor(226, 232, 240)
    highlights = [
        "AI committee reviews every trade proposal",
        "Pattern-based signal detection across FX + equities",
        "Parallel worlds test policy quality before risk",
        "Live IBKR link with real-time position sync",
        "News intelligence with bounded influence",
        "Full audit trail from signal to settlement",
        "Learning ledger closes the feedback loop",
    ]
    for h in highlights:
        p = tf.add_paragraph()
        p.text = f"  {h}"
        p.font.size = Pt(12)
        p.font.color.rgb = RGBColor(180, 195, 215)
        p.space_before = Pt(6)


def _slide_what_is_mip(prs: Presentation) -> None:
    """Slide 2: What Is MIP — the elevator pitch with visual pipeline."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_BG)
    _draw_header(
        slide,
        "What Is MIP?",
        "An evidence-first market intelligence engine — not a prediction machine",
        "OVERVIEW",
    )

    _add_process(
        slide,
        ["Market Data", "Signals", "Training", "Proposals", "Committee", "Execution"],
        y=2.15, start_x=0.5, box_w=1.82, gap=0.25,
    )

    _add_hero_box(
        slide, 0.7, 3.4, 5.9, 3.0, HERO_BLUE_BG,
        "Research Engine",
        [
            "Ingests market data across FX pairs and equities",
            "Detects patterns and evaluates across 5 horizons (H1-H20)",
            "Builds trust scores through backtested evidence",
            "News context + parallel world stress testing",
            "Training maturity gates prevent premature trading",
        ],
        title_color=MIP_BLUE,
    )

    _add_hero_box(
        slide, 6.8, 3.4, 5.9, 3.0, HERO_GREEN_BG,
        "Trading Operations",
        [
            "AI committee verdicts before any execution",
            "Live broker link with IBKR (paper + live modes)",
            "Position lifecycle: import > validate > approve > execute",
            "Performance dashboards with Sharpe, drawdown, win rate",
            "Every decision traceable in the Learning Ledger",
        ],
        title_color=MIP_GREEN,
    )

    _add_analogy_bar(
        slide, "\u2696\uFE0F",
        "Think of MIP as a research lab with a built-in trading desk — the lab must approve before the desk can act.",
    )
    _add_footer(slide)


def _slide_training_status(prs: Presentation) -> None:
    """Slide 3: Training Status."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_BG)
    _draw_header(
        slide,
        "Training Status",
        "Evidence quality is measured before any trade is considered — maturity gates keep the system honest",
        "RESEARCH",
    )

    _add_stat_box(slide, 0.7, 2.15, 2.8, 1.3, "0-100", "Maturity Score", MIP_BLUE)
    _add_stat_box(slide, 3.6, 2.15, 2.8, 1.3, "H1-H20", "5 Horizons", MIP_PURPLE)
    _add_stat_box(slide, 6.5, 2.15, 2.8, 1.3, "86%", "Coverage Target", MIP_GREEN)
    _add_stat_box(slide, 9.4, 2.15, 3.2, 1.3, "PER SYMBOL", "Granularity", MIP_TEAL)

    _add_chip(slide, 0.7, 3.7, 2.3, "INSUFFICIENT", MIP_RED)
    _add_chip(slide, 3.1, 3.7, 2.3, "WARMING UP", MIP_AMBER)
    _add_chip(slide, 5.5, 3.7, 2.3, "LEARNING", MIP_BLUE)
    _add_chip(slide, 7.9, 3.7, 2.3, "CONFIDENT", MIP_GREEN)

    _add_card(
        slide, 0.7, 4.3, 6.0, 2.2,
        "How Training Works",
        [
            "Each symbol + pattern combination is independently trained",
            "Maturity = sample size + coverage + horizon depth",
            "Trust labels gate whether the system can propose trades",
            "LOW_EVIDENCE symbols are quarantined from execution",
            "The system learns continuously — maturity rises over time",
        ],
        edge=MIP_BLUE,
    )

    _add_card(
        slide, 6.85, 4.3, 5.75, 2.2,
        "What You See in the UI",
        [
            "Symbol grid with maturity scores and trust labels",
            "Horizon returns: Avg H1, H3, H5, H10, H20 per pattern",
            "Coverage % — how much of the symbol's history is trained",
            "Sample size — number of observations backing the score",
            "AI summary translates stats into plain-language guidance",
        ],
        edge=MIP_PURPLE,
    )

    _add_analogy_bar(
        slide, "\U0001F393",
        "Like a medical residency: doctors don't operate until they've logged enough supervised hours. MIP's patterns don't trade until trained.",
    )
    _add_footer(slide)


def _slide_market_timeline(prs: Presentation) -> None:
    """Slide 4: Market Timeline."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_BG)
    _draw_header(
        slide,
        "Market Timeline",
        "Symbol-level storyboard — see every signal, proposal, and trade on a visual timeline",
        "RESEARCH",
    )

    _add_process(
        slide,
        ["Signals (S:12)", "Proposals (P:3)", "Trades (T:1)"],
        y=2.15, start_x=1.5, box_w=3.0, gap=0.5,
    )

    _add_chip(slide, 0.7, 3.25, 1.6, "EXECUTED", MIP_GREEN)
    _add_chip(slide, 2.4, 3.25, 1.6, "PROPOSED", MIP_BLUE)
    _add_chip(slide, 4.1, 3.25, 1.8, "SIGNAL ONLY", MIP_AMBER)
    _add_chip(slide, 6.0, 3.25, 1.6, "INACTIVE", RGBColor(113, 128, 150))

    _add_card(
        slide, 0.7, 3.85, 6.0, 2.7,
        "Visual Storyboard",
        [
            "Tile grid colors indicate lifecycle stage at a glance",
            "Chart overlays: signals (blue), proposals (orange), trades (green)",
            "Signal chain tree shows branching proposals per portfolio",
            "OHLC price chart with signal/trade markers overlaid",
            "Cross-check training trust status by pattern directly",
        ],
        edge=MIP_BLUE,
    )

    _add_card(
        slide, 6.85, 3.85, 5.75, 2.7,
        "Narrative Value",
        [
            "Decision narrative explains why a symbol advanced or stalled",
            "Full transparency: signal > proposal > committee > outcome",
            "Click-through to committee decisions and execution runs",
            "Portfolio-level filtering for multi-strategy environments",
            "Time-range controls for historical investigation",
        ],
        edge=MIP_PURPLE,
    )

    _add_analogy_bar(
        slide, "\U0001F4F0",
        "Like a newspaper front page for each symbol — you see the headline (trade), the story (proposals), and the raw sources (signals).",
    )
    _add_footer(slide)


def _slide_news_intelligence(prs: Presentation) -> None:
    """Slide 5: News Intelligence."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_BG)
    _draw_header(
        slide,
        "News Intelligence",
        "Evidence-backed context layer — news informs decisions but never overrides risk gates",
        "RESEARCH",
    )

    _add_stat_box(slide, 0.7, 2.15, 2.85, 1.3, "RSS", "Multi-Source Ingest", MIP_BLUE)
    _add_stat_box(slide, 3.65, 2.15, 2.85, 1.3, "Z \u2265 1.5", "HOT Threshold", MIP_RED)
    _add_stat_box(slide, 6.6, 2.15, 2.85, 1.3, "BOUNDED", "Influence Control", MIP_PURPLE)
    _add_stat_box(slide, 9.55, 2.15, 3.1, 1.3, "REAL-TIME", "Freshness Tracking", MIP_GREEN)

    _add_card(
        slide, 0.7, 3.7, 3.9, 2.8,
        "Context KPIs",
        [
            "Symbols with active news coverage",
            "HOT symbols (burst z-score \u2265 1.5)",
            "Snapshot age and freshness tracking",
            "Stale symbol alerts for decision caution",
        ],
        edge=MIP_BLUE,
    )

    _add_card(
        slide, 4.75, 3.7, 3.9, 2.8,
        "Decision Impact",
        [
            "Proposals scoped with news context",
            "Score adjustments bounded by guardrails",
            "New entry blocks when uncertainty spikes",
            "AI reader summary from stored features",
        ],
        edge=MIP_PURPLE,
    )

    _add_card(
        slide, 8.8, 3.7, 3.85, 2.8,
        "Sources & Guardrails",
        [
            "SEC, GlobeNewswire, MarketWatch, Fed, ECB",
            "IBKR research feed integration",
            "Invalid URLs auto-excluded",
            "No narrative guesswork — evidence only",
        ],
        edge=MIP_GREEN,
    )

    _add_analogy_bar(
        slide, "\U0001F4E1",
        "Like a newsroom fact-checker: MIP reads the headlines but only acts on verified, scored evidence — never on rumor alone.",
    )
    _add_footer(slide)


def _slide_parallel_worlds(prs: Presentation) -> None:
    """Slide 6: Parallel Worlds."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_BG)
    _draw_header(
        slide,
        "Parallel Worlds",
        "Counterfactual laboratory — stress-test policy quality against 'what if' scenarios before risking capital",
        "RESEARCH",
    )

    health = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.7), Inches(2.1), Inches(4.3), Inches(2.0)
    )
    health.fill.solid()
    health.fill.fore_color.rgb = HERO_GREEN_BG
    health.line.color.rgb = RGBColor(165, 214, 167)
    tf = health.text_frame
    tf.word_wrap = True
    tf.text = "Policy Health: HEALTHY"
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.size = Pt(18)
    tf.paragraphs[0].font.color.rgb = MIP_GREEN
    for row in [
        "Stability Score: 95 / 100",
        "Regret Driver: Baseline (staying in cash)",
        "Top Candidate: Weak — continue monitoring",
        "Current policy outperforms all alternatives",
    ]:
        p = tf.add_paragraph()
        p.text = f"  {row}"
        p.font.size = Pt(12)
        p.font.color.rgb = MIP_TEXT
        p.space_before = Pt(3)

    _add_card(
        slide, 5.15, 2.1, 7.5, 2.0,
        "Scenario Types Under Test",
        [
            "THRESHOLD — vary signal filter strictness",
            "SIZING — test alternative position sizing models",
            "TIMING — simulate delayed entry by N bars",
            "HORIZON — change holding period assumptions",
            "EARLY_EXIT — apply payoff multiplier cutoffs",
            "BASELINE — the 'do nothing' cash benchmark",
        ],
        edge=MIP_BLUE,
    )

    _add_chip(slide, 0.7, 4.35, 1.5, "STRONG", MIP_GREEN)
    _add_chip(slide, 2.3, 4.35, 1.7, "EMERGING", MIP_BLUE)
    _add_chip(slide, 4.1, 4.35, 1.3, "WEAK", MIP_AMBER)
    _add_chip(slide, 5.5, 4.35, 1.3, "NOISE", RGBColor(113, 128, 150))

    _add_card(
        slide, 0.7, 4.95, 5.9, 1.55,
        "How It Works",
        [
            "Scenarios are simulated over historical data windows",
            "Gate-by-gate divergence analysis identifies where policies differ",
            "Regret = excess return of best alternative vs current policy",
            "Humans approve all policy changes — AI explains, never decides",
        ],
        edge=MIP_TEAL,
    )

    _add_card(
        slide, 6.75, 4.95, 5.9, 1.55,
        "AI Narrative",
        [
            "AI explains gate-by-gate divergence and regret trend",
            "Generates plain-language summary of scenario performance",
            "Highlights emerging threats to current policy stability",
            "Links to live broker snapshots for real-world validation",
        ],
        edge=MIP_PURPLE,
    )

    _add_analogy_bar(
        slide, "\U0001F30D",
        "Like a flight simulator for your trading strategy — test every 'what-if' scenario before you take off with real capital.",
    )
    _add_footer(slide)


def _slide_live_link(prs: Presentation) -> None:
    """Slide 7: Live Portfolio Link."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_BG)
    _draw_header(
        slide,
        "Live Portfolio Link",
        "The control plane — binding MIP's intelligence engine to real broker execution via Interactive Brokers",
        "TRADING",
    )

    _add_process(
        slide,
        ["Source Portfolio", "MIP Live Config", "IBKR Account"],
        y=2.15, start_x=1.2, box_w=3.2, gap=0.6,
    )

    _add_chip(slide, 4.4, 3.2, 2.0, "Activation Guard", MIP_AMBER)
    _add_chip(slide, 6.6, 3.2, 2.3, "Execution Readiness", MIP_GREEN)

    _add_card(
        slide, 0.7, 3.8, 4.0, 2.7,
        "Configuration",
        [
            "Adapter mode: PAPER or LIVE",
            "Max positions and exposure limits",
            "Cash buffer percentage",
            "Quote freshness thresholds",
            "Cooldown bars between trades",
        ],
        edge=MIP_BLUE,
    )

    _add_card(
        slide, 4.85, 3.8, 4.0, 2.7,
        "Risk Brakes",
        [
            "Drawdown stop threshold",
            "Bust percentage limit",
            "Validity window enforcement",
            "Snapshot freshness gates",
            "Position-level safeguards",
        ],
        edge=MIP_RED,
    )

    _add_card(
        slide, 9.0, 3.8, 3.65, 2.7,
        "Governance",
        [
            "Save writes config — no orders",
            "Execution requires validation",
            "Committee must approve first",
            "Full audit of every state change",
            "Human override at every step",
        ],
        edge=MIP_GREEN,
    )

    _add_analogy_bar(
        slide, "\U0001F3DB\uFE0F",
        "Like air traffic control: MIP clears each flight (trade) only when conditions, fuel (capital), and weather (market) all pass safety checks.",
    )
    _add_footer(slide)


def _slide_live_activity(prs: Presentation) -> None:
    """Slide 8: Live Portfolio Activity."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_BG)
    _draw_header(
        slide,
        "Live Portfolio Activity",
        "Real-time operational lifecycle with committee checkpoints at every stage",
        "TRADING",
    )

    _add_process(
        slide,
        ["Imported", "Validated", "Committee", "Approved", "Executed"],
        y=2.15, start_x=0.65, box_w=2.15, gap=0.32,
    )

    _add_chip(slide, 4.95, 3.2, 2.8, "Committee Checkpoint", MIP_PURPLE)

    _add_stat_box(slide, 0.7, 3.75, 3.0, 1.2, "NAV", "Net Asset Value", MIP_GREEN)
    _add_stat_box(slide, 3.85, 3.75, 3.0, 1.2, "P&L", "Unrealized Gains", MIP_BLUE)
    _add_stat_box(slide, 7.0, 3.75, 3.0, 1.2, "DRIFT", "Alignment Check", MIP_AMBER)
    _add_stat_box(slide, 10.15, 3.75, 2.5, 1.2, "W / L", "Winners vs Losers", MIP_PURPLE)

    _add_card(
        slide, 0.7, 5.2, 6.0, 1.4,
        "What You Monitor",
        [
            "Open positions: symbol, side, qty, avg cost, unrealized P&L",
            "Execution history: fill prices, realized P&L, timestamps",
            "TP/SL status + protection level for each position",
        ],
        edge=MIP_BLUE,
    )

    _add_card(
        slide, 6.85, 5.2, 5.75, 1.4,
        "Cross-Checks Available",
        [
            "AI Agent Decisions  for verdict context and reason codes",
            "Runs  for execution truth and pipeline health",
            "Symbol Tracker  for real-time thesis validation",
        ],
        edge=MIP_PURPLE,
    )

    _add_analogy_bar(
        slide, "\U0001F3E5",
        "Like a hospital patient monitor — vital signs (NAV, P&L, drift) are tracked continuously, and alerts fire before problems escalate.",
    )
    _add_footer(slide)


def _slide_ai_decisions(prs: Presentation) -> None:
    """Slide 9: AI Agent Decisions."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_BG)
    _draw_header(
        slide,
        "AI Agent Decisions",
        "The committee courtroom — every trade proposal faces a structured verdict with reason codes",
        "DECISIONS",
    )

    _add_card(
        slide, 0.7, 2.15, 5.9, 2.3,
        "APPROVED Example",
        [
            "Verdict:  APPROVE",
            "Confidence:  HIGH",
            "Trust gate:  PASSED — maturity 82, coverage 91%",
            "Risk gate:  PASSED — within position limits",
            "Freshness:  PASSED — quote age 3 min",
            "Committee summary generated for audit trail",
        ],
        edge=MIP_GREEN,
    )

    _add_card(
        slide, 6.75, 2.15, 5.9, 2.3,
        "REJECTED Example",
        [
            "Verdict:  REJECT",
            "Confidence:  MEDIUM",
            "Trust gate:  FAILED — maturity only 34",
            "Capacity gate:  BLOCKED — max positions reached",
            "Stale quote risk flagged (15 min age)",
            "Reason codes preserved for learning ledger",
        ],
        edge=MIP_RED,
    )

    _add_process(
        slide,
        ["PROPOSED", "TRUST GATE", "RISK GATE", "COMMITTEE", "VERDICT"],
        y=4.75, start_x=0.65, box_w=2.15, gap=0.32,
    )

    _add_card(
        slide, 0.7, 5.85, 11.9, 0.7,
        "Deterministic Accountability",
        [
            "Reason codes are structured and queryable — not free-text opinions. Every APPROVE and REJECT is reproducible and auditable.",
        ],
        edge=MIP_PURPLE, item_size=13,
    )

    _add_analogy_bar(
        slide, "\u2696\uFE0F",
        "Like a courtroom trial: evidence is presented, gates are checked, and a structured verdict is rendered — with a full transcript for appeal.",
    )
    _add_footer(slide)


def _slide_learning_ledger(prs: Presentation) -> None:
    """Slide 10: Learning Ledger."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_BG)
    _draw_header(
        slide,
        "Learning Ledger",
        "The institutional memory — every decision, its context, and what happened next",
        "REVIEW",
    )

    _add_process(
        slide,
        ["Proposal", "Decision", "Execution", "Outcome", "Lesson"],
        y=2.15, start_x=0.65, box_w=2.15, gap=0.32,
    )

    _add_card(
        slide, 0.7, 3.35, 4.0, 3.1,
        "Event Types",
        [
            "TRAINING_EVENT",
            "  Maturity changes, trust label shifts",
            "DECISION_EVENT",
            "  Committee verdicts, reason codes",
            "LIVE_EVENT",
            "  Execution fills, P&L outcomes",
        ],
        edge=MIP_BLUE,
    )

    _add_card(
        slide, 4.85, 3.35, 3.9, 3.1,
        "Causality Chain",
        [
            "Before State  vs  After State",
            "Influence delta tracking",
            "Causality links between events",
            "Outcome state for closed-loop review",
            "News context influence measured",
        ],
        edge=MIP_TEAL,
    )

    _add_card(
        slide, 8.9, 3.35, 3.75, 3.1,
        "Team Use Cases",
        [
            "Post-trade retrospectives",
            "Weekly performance reviews",
            "Policy tuning decisions",
            "Rejection pattern analysis",
            "Stakeholder explainability",
        ],
        edge=MIP_PURPLE,
    )

    _add_analogy_bar(
        slide, "\U0001F4DA",
        "Like a pilot's flight log: every takeoff, turbulence, and landing is recorded — so the next flight benefits from every past experience.",
    )
    _add_footer(slide)


def _slide_symbol_tracker(prs: Presentation) -> None:
    """Slide 11: Live Symbol Tracker — dark theme."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, RGBColor(11, 18, 32))
    _draw_header(
        slide,
        "Live Symbol Tracker",
        "Real-time thesis monitoring with committee commentary on every open position",
        "MONITORING",
    )

    _add_chip(slide, 0.7, 2.15, 2.2, "THESIS INTACT", RGBColor(3, 105, 70))
    _add_chip(slide, 3.05, 2.15, 2.0, "WEAKENING", MIP_AMBER)
    _add_chip(slide, 5.2, 2.15, 2.1, "INVALIDATED", MIP_RED)
    _add_chip(slide, 7.45, 2.15, 2.5, "PROTECTED FULL", MIP_BLUE)

    _add_dark_card(
        slide, 0.7, 2.75, 6.0, 3.7,
        "Symbol Metrics (Live)",
        [
            "Open R:  +1.2R  (reward vs risk from entry)",
            "Expected Move Reached:  84%",
            "Distance to Stop Loss:  1.6%",
            "Distance to Take Profit:  0.8%",
            "Vol Regime:  Normal \u2192 Elevated",
            "Protection Status:  PROTECTED_PARTIAL",
            "",
            "Price chart with TP/SL bands overlaid",
            "Real-time quote updates from IBKR feed",
        ],
        edge=MIP_BLUE,
    )

    _add_dark_card(
        slide, 6.85, 2.75, 5.85, 3.7,
        "Committee Commentary (Live)",
        [
            "Stance:  WATCH_CLOSELY",
            "Confidence:  MEDIUM",
            "",
            "Reason Tags:",
            "  momentum_fade, near_tp, news_uncertainty",
            "",
            "Suggested Actions:",
            "  Tighten stop, monitor revalidation window",
            "  Consider partial exit if thesis weakens further",
        ],
        edge=MIP_PURPLE,
    )

    bar = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.7), Inches(6.6), Inches(11.9), Inches(0.5)
    )
    bar.fill.solid()
    bar.fill.fore_color.rgb = RGBColor(20, 32, 50)
    bar.line.color.rgb = RGBColor(40, 58, 90)
    tf = bar.text_frame
    tf.text = "\U0001F4E1  Like a mission control dashboard: every satellite (position) has live telemetry and a flight director's commentary."
    tf.paragraphs[0].font.size = Pt(13)
    tf.paragraphs[0].font.italic = True
    tf.paragraphs[0].font.color.rgb = RGBColor(148, 163, 184)

    _add_footer(slide, text="MIP  |  Symbol monitoring + real-time committee commentary")


def _slide_performance(prs: Presentation) -> None:
    """Slide 12: Performance Dashboard."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_BG)
    _draw_header(
        slide,
        "Performance Dashboard",
        "Portfolio-level truth — return, risk, consistency, and the story behind the numbers",
        "REVIEW",
    )

    _add_stat_box(slide, 0.7, 2.15, 2.9, 1.3, "+12.4%", "Total Return", MIP_GREEN)
    _add_stat_box(slide, 3.75, 2.15, 2.9, 1.3, "-4.1%", "Max Drawdown", MIP_RED)
    _add_stat_box(slide, 6.8, 2.15, 2.9, 1.3, "1.82", "Sharpe Ratio", MIP_BLUE)
    _add_stat_box(slide, 9.85, 2.15, 2.8, 1.3, "67%", "Win Rate", MIP_PURPLE)

    _add_card(
        slide, 0.7, 3.7, 4.0, 2.8,
        "Core Metrics",
        [
            "Total equity and return %",
            "Max drawdown and recovery time",
            "Sharpe ratio for risk-adjusted returns",
            "Win rate and expectancy per trade",
            "Monthly cost trend analysis",
        ],
        edge=MIP_GREEN,
    )

    _add_card(
        slide, 4.85, 3.7, 4.0, 2.8,
        "Visual Analysis",
        [
            "Equity curve with drawdown bands",
            "Decision quality trend (expectancy over time)",
            "Selectivity trend — how filters evolve",
            "Decision funnel: signals > proposals > trades",
            "Parallel Worlds counterfactual comparison",
        ],
        edge=MIP_BLUE,
    )

    _add_card(
        slide, 9.0, 3.7, 3.65, 2.8,
        "Drill-Down Paths",
        [
            "Period filters for focused analysis",
            "Compare return vs drawdown across portfolios",
            "Trace root cause in Training + Decisions",
            "Target realism analysis",
            "Cost attribution breakdown",
        ],
        edge=MIP_PURPLE,
    )

    _add_analogy_bar(
        slide, "\U0001F4CA",
        "Like a financial annual report — but updated daily, with drill-through to every decision that shaped the bottom line.",
    )
    _add_footer(slide)


def _slide_runs(prs: Presentation) -> None:
    """Slide 13: Runs / Audit Trail."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_BG)
    _draw_header(
        slide,
        "Pipeline Runs & Audit Trail",
        "Operational truth — what ran, when, how long, and what went wrong (if anything)",
        "OPERATIONS",
    )

    _add_chip(slide, 0.7, 2.15, 1.5, "SUCCESS", MIP_GREEN)
    _add_chip(slide, 2.3, 2.15, 2.3, "SUCCESS + SKIPS", MIP_AMBER)
    _add_chip(slide, 4.7, 2.15, 1.3, "FAILED", MIP_RED)
    _add_chip(slide, 6.1, 2.15, 1.5, "RUNNING", MIP_BLUE)

    _add_card(
        slide, 0.7, 2.75, 6.0, 2.1,
        "Daily Pipeline Sequence",
        [
            "Ingest market data \u2192 Generate signals \u2192 Evaluate patterns",
            "Run portfolio simulation \u2192 Propose trades \u2192 Execute approved",
            "Generate briefs + digest \u2192 Run parallel worlds analysis",
            "Scheduled: 5 PM ET, Monday through Friday",
        ],
        edge=MIP_BLUE,
    )

    _add_card(
        slide, 6.85, 2.75, 5.8, 2.1,
        "Run Detail Panel",
        [
            "Summary cards: status, duration, as-of date, portfolio count",
            "Step-by-step timeline with individual durations",
            "Error panel: SQLSTATE, query IDs, error messages",
            "AI-generated run summary accelerates triage",
        ],
        edge=MIP_PURPLE,
    )

    _add_card(
        slide, 0.7, 5.1, 11.9, 1.4,
        "Audit Log Architecture",
        [
            "MIP_AUDIT_LOG captures every event: stored procedure name, status, duration, error details, and portfolio context.",
            "Immutable audit trail — entries are append-only. Failed runs preserve full diagnostics for root cause analysis.",
            "AI summary explains what happened in plain language — but step-level detail is always available for verification.",
        ],
        edge=MIP_TEAL, item_size=12,
    )

    _add_analogy_bar(
        slide, "\u2708\uFE0F",
        "Like an aircraft's black box recorder: every system event is logged, so investigations start with facts, not guesses.",
    )
    _add_footer(slide)


def _slide_platform_architecture(prs: Presentation) -> None:
    """Slide 14: Platform architecture overview."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_BG)
    _draw_header(
        slide,
        "Platform Architecture",
        "Snowflake-native: data, logic, and pipelines live together — the UI and API are thin presentation layers",
        "ARCHITECTURE",
    )

    _add_icon_circle(slide, 1.5, 2.3, 1.2, "SF", MIP_BLUE, font_size=24)
    t = slide.shapes.add_textbox(Inches(1.2), Inches(3.6), Inches(1.8), Inches(0.4))
    tf = t.text_frame
    tf.text = "Snowflake"
    tf.paragraphs[0].alignment = PP_ALIGN.CENTER
    tf.paragraphs[0].font.size = Pt(13)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = MIP_BLUE

    _add_icon_circle(slide, 4.5, 2.3, 1.2, "API", MIP_PURPLE, font_size=20)
    t = slide.shapes.add_textbox(Inches(4.2), Inches(3.6), Inches(1.8), Inches(0.4))
    tf = t.text_frame
    tf.text = "Python API"
    tf.paragraphs[0].alignment = PP_ALIGN.CENTER
    tf.paragraphs[0].font.size = Pt(13)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = MIP_PURPLE

    _add_icon_circle(slide, 7.5, 2.3, 1.2, "UI", MIP_GREEN, font_size=24)
    t = slide.shapes.add_textbox(Inches(7.2), Inches(3.6), Inches(1.8), Inches(0.4))
    tf = t.text_frame
    tf.text = "React UI"
    tf.paragraphs[0].alignment = PP_ALIGN.CENTER
    tf.paragraphs[0].font.size = Pt(13)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = MIP_GREEN

    _add_icon_circle(slide, 10.5, 2.3, 1.2, "IB", MIP_AMBER, font_size=24)
    t = slide.shapes.add_textbox(Inches(10.2), Inches(3.6), Inches(1.8), Inches(0.4))
    tf = t.text_frame
    tf.text = "IBKR"
    tf.paragraphs[0].alignment = PP_ALIGN.CENTER
    tf.paragraphs[0].font.size = Pt(13)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = MIP_AMBER

    for x_start in [2.85, 5.85, 8.85]:
        arrow = slide.shapes.add_shape(
            MSO_SHAPE.RIGHT_ARROW, Inches(x_start), Inches(2.7), Inches(1.4), Inches(0.4)
        )
        arrow.fill.solid()
        arrow.fill.fore_color.rgb = RGBColor(183, 194, 214)
        arrow.line.fill.background()

    layers = [
        ("Data Layer", "Tables, views, stored procedures, tasks, external stages", MIP_BLUE),
        ("Logic Layer", "Signal detection, training, proposals, committee, simulation, parallel worlds", MIP_PURPLE),
        ("Presentation", "React pages, REST API routers, WebSocket feeds, chart overlays", MIP_GREEN),
        ("Integration", "IBKR adapter, RSS ingest, broker snapshots, execution bridge", MIP_AMBER),
    ]
    for i, (title, desc, color) in enumerate(layers):
        _add_card(
            slide, 0.7 + i * 3.15, 4.2, 3.0, 2.3,
            title, [desc], edge=color, item_size=11,
        )

    _add_footer(slide)


def _slide_closing(prs: Presentation) -> None:
    """Slide 15: Closing / CTA."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _set_bg(slide, MIP_DARK)

    banner = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, Inches(0), Inches(0), SLIDE_W, SLIDE_H
    )
    banner.fill.solid()
    banner.fill.fore_color.rgb = MIP_DARK
    banner.line.fill.background()

    accent = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, Inches(0.9), Inches(1.0), Inches(0.15), Inches(5.5)
    )
    accent.fill.solid()
    accent.fill.fore_color.rgb = MIP_BLUE
    accent.line.fill.background()

    title = slide.shapes.add_textbox(Inches(1.3), Inches(1.2), Inches(10.5), Inches(1.2))
    tf = title.text_frame
    tf.text = "MIP: Intelligence Meets Execution"
    tf.paragraphs[0].font.size = Pt(44)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = RGBColor(241, 245, 249)

    sub = slide.shapes.add_textbox(Inches(1.3), Inches(2.5), Inches(10.0), Inches(0.6))
    tf = sub.text_frame
    tf.text = "The platform where every trade is born from evidence, governed by committee, and remembered for the future."
    tf.paragraphs[0].font.size = Pt(18)
    tf.paragraphs[0].font.color.rgb = RGBColor(148, 163, 184)

    divider = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, Inches(1.3), Inches(3.3), Inches(4.0), Inches(0.04)
    )
    divider.fill.solid()
    divider.fill.fore_color.rgb = MIP_BLUE
    divider.line.fill.background()

    pillars_data = [
        ("\U0001F52C", "Research", "Pattern detection, training maturity, news intelligence, parallel worlds"),
        ("\u2696\uFE0F", "Decisions", "AI committee verdicts, reason codes, structured gates, full transparency"),
        ("\U0001F4B9", "Execution", "Live IBKR link, position lifecycle, risk brakes, real-time sync"),
        ("\U0001F4DA", "Learning", "Causal ledger, outcome tracking, policy feedback, continuous improvement"),
    ]

    y = 3.6
    for icon, pillar, desc in pillars_data:
        row = slide.shapes.add_textbox(Inches(1.3), Inches(y), Inches(10.0), Inches(0.55))
        tf = row.text_frame
        tf.word_wrap = True
        tf.text = f"{icon}  {pillar}  —  {desc}"
        tf.paragraphs[0].font.size = Pt(16)
        tf.paragraphs[0].font.color.rgb = RGBColor(203, 213, 225)
        y += 0.65

    cta = slide.shapes.add_textbox(Inches(1.3), Inches(6.2), Inches(10.0), Inches(0.5))
    tf = cta.text_frame
    tf.text = "Ready to see it live?  Let's walk through the platform together."
    tf.paragraphs[0].font.size = Pt(20)
    tf.paragraphs[0].font.bold = True
    tf.paragraphs[0].font.color.rgb = MIP_BLUE


# ══════════════════════════════════════════════════════════════════════════════
#  BUILD DECK
# ══════════════════════════════════════════════════════════════════════════════

def build_deck(output_path: Path) -> None:
    prs = Presentation()
    prs.slide_width = SLIDE_W
    prs.slide_height = SLIDE_H

    _slide_title(prs)               # 1 - Title / Hero
    _slide_what_is_mip(prs)         # 2 - What is MIP
    _slide_training_status(prs)     # 3 - Training Status
    _slide_market_timeline(prs)     # 4 - Market Timeline
    _slide_news_intelligence(prs)   # 5 - News Intelligence
    _slide_parallel_worlds(prs)     # 6 - Parallel Worlds
    _slide_live_link(prs)           # 7 - Live Portfolio Link
    _slide_live_activity(prs)       # 8 - Live Portfolio Activity
    _slide_ai_decisions(prs)        # 9 - AI Agent Decisions
    _slide_learning_ledger(prs)     # 10 - Learning Ledger
    _slide_symbol_tracker(prs)      # 11 - Live Symbol Tracker
    _slide_performance(prs)         # 12 - Performance
    _slide_runs(prs)                # 13 - Runs / Audit
    _slide_platform_architecture(prs)  # 14 - Architecture
    _slide_closing(prs)             # 15 - Closing / CTA

    output_path.parent.mkdir(parents=True, exist_ok=True)
    prs.save(output_path)


if __name__ == "__main__":
    deck_path = Path(__file__).with_name("MIP_Platform_Introduction.pptx")
    build_deck(deck_path)
    print(f"Created: {deck_path}")
