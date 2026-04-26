"""
Cockpit composition services.

These modules build the data the operator-facing /cockpit/overview
endpoint returns. They reuse the live-broker view, the daily Position
Health views, and the on-demand intraday overlay. Nothing here depends
on horizon/sim sources (PORTFOLIO_POSITIONS / PORTFOLIO_TRADES /
V_PORTFOLIO_OPEN_POSITIONS_CANONICAL).
"""
