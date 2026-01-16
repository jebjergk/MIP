import streamlit as st


COMPACT_CSS = """
<style>
html, body, [class*="css"]  {
    font-size: 14px;
}
.stMetric {
    background: #f7f9fb;
    border: 1px solid #e4e7ec;
    padding: 0.6rem 0.75rem;
    border-radius: 0.5rem;
}
.stMetric label {
    font-size: 0.75rem;
    color: #667085;
}
.block-container {
    padding-top: 1.5rem;
    padding-bottom: 2rem;
}
.section-header {
    margin-top: 0.6rem;
    margin-bottom: 0.2rem;
}
.badge {
    display: inline-block;
    padding: 0.15rem 0.45rem;
    border-radius: 999px;
    font-size: 0.7rem;
    background: #eef2ff;
    color: #3538cd;
    border: 1px solid #d0d5ff;
}
.badge-warning {
    background: #fef3f2;
    color: #b42318;
    border-color: #fecdca;
}
</style>
"""


def apply_layout(title: str, caption: str | None = None) -> None:
    st.set_page_config(page_title=title, layout="wide", initial_sidebar_state="collapsed")
    st.markdown(COMPACT_CSS, unsafe_allow_html=True)
    st.title(title)
    if caption:
        st.caption(caption)


def section_header(title: str, caption: str | None = None) -> None:
    st.markdown(f"#### <span class='section-header'>{title}</span>", unsafe_allow_html=True)
    if caption:
        st.caption(caption)


def render_badge(label: str, variant: str = "default") -> None:
    class_name = "badge"
    if variant == "warning":
        class_name += " badge-warning"
    st.markdown(f"<span class='{class_name}'>{label}</span>", unsafe_allow_html=True)
