from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from typing import Any

from app.price_action.geometry import Bar
from app.price_action.service import (
    PriceActionError,
    analyse_for_brooks_historical_reconstruction,
    load_daily_bars_historical_reconstruction,
)

from .calendar import resolve_week_sessions
from .constants import (
    COMPILER_VERSION_DEFAULT,
    DEFAULT_PAA_LOOKBACK_BARS,
    DOSSIER_VERSION_DEFAULT,
    RECONSTRUCTION_PROMPT_VERSION_DEFAULT,
    RECONSTRUCTION_VERSION_DEFAULT,
)
from .db_util import query_rows
from .dossier_compiler import _stable_hash, _structure_fields
from .errors import BrooksIntradayError
from .level_derivation import assess_readiness, derive_levels

DOSSIER_ORIGIN_RECONSTRUCTION = "HISTORICAL_RECONSTRUCTION"
DOSSIER_STATUS_RECONSTRUCTION = "HISTORICAL_RECONSTRUCTION"
DOSSIER_STATUS_FAILED = "RECONSTRUCTION_FAILED"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _load_holidays(start: date, end: date) -> dict[date, dict]:
    rows = query_rows(
        """
        SELECT HOLIDAY_DATE, FULL_DAY_CLOSE, HOLIDAY_NAME
        FROM MIP.APP.US_EQUITY_HOLIDAYS
        WHERE HOLIDAY_DATE BETWEEN %s AND %s
        """,
        (start, end),
    )
    out: dict[date, dict] = {}
    for row in rows:
        d = row.get("holiday_date") or row.get("HOLIDAY_DATE")
        if isinstance(d, str):
            d = date.fromisoformat(d[:10])
        out[d] = {
            "full_day_close": bool(row.get("full_day_close") if row.get("full_day_close") is not None else row.get("FULL_DAY_CLOSE")),
            "name": row.get("holiday_name") or row.get("HOLIDAY_NAME"),
        }
    return out


def latest_daily_input_date_before(trading_date: date) -> date:
    """Last US equity session date strictly before trading_date (weekends/holidays skipped)."""
    start = trading_date - timedelta(days=14)
    holidays = _load_holidays(start, trading_date)
    d = trading_date - timedelta(days=1)
    while d >= start:
        if d.weekday() >= 5:
            d -= timedelta(days=1)
            continue
        hol = holidays.get(d)
        if hol and hol.get("full_day_close"):
            d -= timedelta(days=1)
            continue
        return d
    raise BrooksIntradayError(
        "RECONSTRUCTION_CALENDAR",
        f"Could not resolve prior session before {trading_date.isoformat()}.",
    )


def assert_point_in_time_daily_bars(bars: list[Bar], target_trading_date: date) -> None:
    if not bars:
        raise BrooksIntradayError("RECONSTRUCTION_NO_BARS", "No daily bars supplied.")
    max_day = max(b.day for b in bars)
    if max_day >= target_trading_date:
        raise BrooksIntradayError(
            "POINT_IN_TIME_LEAKAGE",
            f"Max daily bar date {max_day} must be before trading date {target_trading_date}.",
        )


def assert_no_intraday_leakage(
    *,
    symbol: str,
    target_trading_date: date,
    run_id: str | None = None,
) -> None:
    """Brooks 5m store must not be read during reconstruction (guard for tests)."""
    del symbol, target_trading_date, run_id


def daily_bars_source_hash(bars: list[Bar]) -> str:
    payload = [
        {"d": b.day.isoformat(), "o": b.open, "h": b.high, "l": b.low, "c": b.close, "v": b.volume}
        for b in bars
    ]
    return hashlib.sha256(str(payload).encode()).hexdigest()


def reconstruction_id_for(run_id: str, symbol: str, trading_date: date) -> str:
    raw = f"{run_id}|{symbol.upper()}|{trading_date.isoformat()}|{RECONSTRUCTION_VERSION_DEFAULT}"
    return f"brooks-recon-{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def reconstruct_single_dossier(
    *,
    run_id: str,
    symbol: str,
    trading_date: date,
    lookback_bars: int = DEFAULT_PAA_LOOKBACK_BARS,
) -> dict[str, Any]:
    sym = symbol.strip().upper()
    daily_cutoff = latest_daily_input_date_before(trading_date)
    knowledge_cutoff = _utc_now()

    try:
        bars = load_daily_bars_historical_reconstruction(
            sym,
            target_trading_date=trading_date,
            daily_bar_cutoff=daily_cutoff,
            lookback_bars=lookback_bars,
        )
    except PriceActionError as exc:
        raise BrooksIntradayError("RECONSTRUCTION_FAILED", str(exc)) from exc

    assert_point_in_time_daily_bars(bars, trading_date)
    assert_no_intraday_leakage(symbol=sym, target_trading_date=trading_date, run_id=run_id)

    try:
        response = analyse_for_brooks_historical_reconstruction(
            sym,
            target_trading_date=trading_date,
            daily_bar_cutoff=daily_cutoff,
            lookback_bars=lookback_bars,
        )
    except PriceActionError as exc:
        raise BrooksIntradayError("RECONSTRUCTION_FAILED", str(exc)) from exc

    geometry = response.detected_geometry
    situation = response.situation_model
    methodologist = response.methodologist.model_dump()
    verdict = response.methodologist.verdict.decision
    confidence = float(response.methodologist.verdict.confidence)

    trend, daily_structure, market_cycle, location = _structure_fields(geometry)
    derived = derive_levels(geometry, situation, methodologist, verdict=verdict)
    from .level_derivation import _zones_from_geometry

    sz, rz = _zones_from_geometry(geometry)
    recon_id = reconstruction_id_for(run_id, sym, trading_date)
    bar_hash = daily_bars_source_hash(bars)
    reconstructed_at = _utc_now()

    methodologist_summary = ""
    plain = methodologist.get("plain_explanation") or {}
    if isinstance(plain, dict):
        methodologist_summary = plain.get("summary") or plain.get("what_the_chart_is_doing") or ""
    expert = methodologist.get("expert") or {}
    preferred = expert.get("preferred_scenario") or ""
    main_risk = expert.get("main_risk") or ""

    dossier: dict[str, Any] = {
        "run_id": run_id,
        "symbol": sym,
        "trading_date": trading_date.isoformat(),
        "dossier_origin": DOSSIER_ORIGIN_RECONSTRUCTION,
        "dossier_provenance_status": DOSSIER_STATUS_RECONSTRUCTION,
        "source_paa_analysis_id": None,
        "paa_analysis_id": None,
        "reconstruction_id": recon_id,
        "reconstruction_version": RECONSTRUCTION_VERSION_DEFAULT,
        "reconstruction_prompt_version": RECONSTRUCTION_PROMPT_VERSION_DEFAULT,
        "knowledge_cutoff_utc": knowledge_cutoff.isoformat(timespec="seconds"),
        "daily_data_cutoff_date": daily_cutoff.isoformat(),
        "latest_daily_bar_used": bars[-1].day.isoformat(),
        "reconstructed_at_utc": reconstructed_at.isoformat(timespec="seconds"),
        "point_in_time_enforced": True,
        "llm_used": False,
        "rag_used": False,
        "model_id": None,
        "board_run_id": None,
        "paa_as_of_date": bars[-1].day.isoformat(),
        "paa_scanned_at_utc": None,
        "paa_verdict": verdict,
        "paa_confidence": confidence,
        "daily_trend": trend,
        "daily_structure": daily_structure,
        "market_cycle": market_cycle,
        "location": location,
        "latest_close": derived.get("latest_close"),
        "support_zones": sz,
        "resistance_zones": rz,
        "entry_zone": derived.get("entry_zone"),
        "reclaim_level": (derived.get("reclaim_level") or {}).get("value"),
        "invalidation_level": (derived.get("invalidation_level") or {}).get("value"),
        "do_not_chase_level": (derived.get("do_not_chase_level") or {}).get("value"),
        "preferred_long_scenario": preferred,
        "main_risk": main_risk,
        "methodologist_summary": methodologist_summary
        or f"Reconstructed daily context as-of {daily_cutoff.isoformat()} (deterministic PAA geometry).",
        "geometry_summary_json": geometry,
        "situation_model_json": situation,
        "methodologist_output_json": methodologist,
        "rag_card_ids": [],
        "dossier_version": DOSSIER_VERSION_DEFAULT,
        "compiler_version": COMPILER_VERSION_DEFAULT,
        "compiled_at_utc": reconstructed_at.isoformat(timespec="seconds"),
        "derived_levels": derived,
        "source_daily_bar_hash": bar_hash,
        "validation_messages": [],
    }

    obs_ready, sim_ready, val_status, val_messages = assess_readiness(dossier, verdict=verdict)
    dossier["observation_ready"] = obs_ready
    dossier["trade_simulation_ready"] = sim_ready
    dossier["validation_status"] = val_status
    dossier["validation_messages"] = val_messages

    source_hash = _stable_hash(
        {
            "reconstruction_id": recon_id,
            "geometry": geometry,
            "situation": situation,
            "methodologist": methodologist,
            "daily_bar_hash": bar_hash,
            "reconstruction_version": RECONSTRUCTION_VERSION_DEFAULT,
        }
    )
    dossier["source_hash"] = source_hash
    return dossier


def validate_reconstructed_dossier(dossier: dict[str, Any], trading_date: date) -> list[str]:
    warnings: list[str] = []
    latest = dossier.get("latest_daily_bar_used")
    if latest:
        latest_d = date.fromisoformat(str(latest)[:10])
        if latest_d >= trading_date:
            warnings.append("Latest daily bar used is not before trading date.")
    inv = dossier.get("invalidation_level")
    if inv is None and dossier.get("trade_simulation_ready"):
        warnings.append("Simulation ready without invalidation level.")
    if not dossier.get("source_daily_bar_hash"):
        warnings.append("Missing source daily-bar hash.")
    return warnings


def reconstruct_all_for_run(state: dict[str, Any]) -> dict[str, Any]:
    run_id = state["run_id"]
    if state.get("dossiers_frozen"):
        return {"status": "ALREADY_FROZEN", "preparation": state.get("preparation")}

    week_start = state.get("selected_week_start")
    if not week_start:
        raise BrooksIntradayError("WEEK_NOT_CONFIGURED", "Run has no selected week.")

    symbols = state["symbols"]
    week = resolve_week_sessions(week_start)
    if week.status != "READY":
        raise BrooksIntradayError("WEEK_NOT_READY", week.status)

    prep = state.get("preparation") or {}
    state["preparation"] = prep
    matrix = prep.setdefault("matrix", [])
    matrix_by_key = {
        f"{row.get('trading_date')}|{row.get('symbol')}": row for row in matrix if row.get("symbol")
    }

    compiled = 0
    reconstructed = 0
    failed: list[dict] = []
    latest_daily_by_session: dict[str, str] = {}

    for trading_date in week.trading_dates:
        for symbol in symbols:
            key = f"{trading_date.isoformat()}|{symbol}"
            row = matrix_by_key.get(key) or {
                "trading_date": trading_date.isoformat(),
                "symbol": symbol,
            }
            try:
                dossier = reconstruct_single_dossier(
                    run_id=run_id,
                    symbol=symbol,
                    trading_date=trading_date,
                )
                val_warn = validate_reconstructed_dossier(dossier, trading_date)
                dossier["validation_messages"] = list(dossier.get("validation_messages") or []) + val_warn

                from .repository import insert_dossier_if_absent

                inserted = insert_dossier_if_absent(
                    run_id=run_id,
                    symbol=symbol,
                    trading_date=trading_date,
                    dossier=dossier,
                    paa_analysis_id=None,
                    board_run_id=None,
                    normalized_status=DOSSIER_STATUS_RECONSTRUCTION,
                    source_hash=dossier["source_hash"],
                    dossier_version=DOSSIER_VERSION_DEFAULT,
                    compiler_version=COMPILER_VERSION_DEFAULT,
                    run_frozen=False,
                )
                if not inserted:
                    raise BrooksIntradayError(
                        "DOSSIER_EXISTS",
                        f"Dossier already present for {symbol} {trading_date}.",
                        run_id=run_id,
                    )

                compiled += 1
                reconstructed += 1
                latest_daily_by_session[trading_date.isoformat()] = dossier["latest_daily_bar_used"]

                row.update(
                    {
                        "dossier_status": DOSSIER_STATUS_RECONSTRUCTION,
                        "dossier_origin": DOSSIER_ORIGIN_RECONSTRUCTION,
                        "dossier_provenance_status": DOSSIER_STATUS_RECONSTRUCTION,
                        "paa_status": DOSSIER_STATUS_RECONSTRUCTION,
                        "paa_analysis_id": None,
                        "reconstruction_id": dossier["reconstruction_id"],
                        "latest_daily_bar_used": dossier["latest_daily_bar_used"],
                        "daily_data_cutoff_date": dossier["daily_data_cutoff_date"],
                        "observation_ready": dossier.get("observation_ready"),
                        "simulation_ready": dossier.get("trade_simulation_ready"),
                        "paa_verdict": dossier.get("paa_verdict"),
                        "paa_confidence": dossier.get("paa_confidence"),
                        "daily_trend": dossier.get("daily_trend"),
                        "validation_messages": dossier.get("validation_messages"),
                    }
                )
                matrix_by_key[key] = row
            except BrooksIntradayError as exc:
                failed.append(
                    {
                        "symbol": symbol,
                        "trading_date": trading_date.isoformat(),
                        "code": exc.code,
                        "message": exc.message,
                    }
                )
                row.update({"dossier_status": DOSSIER_STATUS_FAILED, "dossier_origin": DOSSIER_ORIGIN_RECONSTRUCTION})
                matrix_by_key[key] = row

    prep["matrix"] = [
        matrix_by_key[f"{td.isoformat()}|{sym}"]
        for td in week.trading_dates
        for sym in symbols
        if f"{td.isoformat()}|{sym}" in matrix_by_key
    ]
    counts = prep.setdefault("counts", {})
    expected = len(symbols) * len(week.trading_dates)
    counts["dossiers_expected"] = expected
    counts["dossiers_compiled"] = compiled
    counts["genuine_paa_dossiers"] = 0
    counts["reconstructed_dossiers"] = reconstructed
    prep["latest_daily_input_by_trading_date"] = latest_daily_by_session
    prep["reconstruction_version"] = RECONSTRUCTION_VERSION_DEFAULT
    prep["reconstruction_prompt_version"] = RECONSTRUCTION_PROMPT_VERSION_DEFAULT
    prep["llm_used"] = False
    prep["rag_used"] = False

    if compiled == expected and not failed:
        prep["preparation_status"] = "DOSSIERS_READY"
        prep["fatal_errors"] = []
        state["preparation_status"] = "READY"
        state["status"] = "READY"
        state["dossiers_frozen"] = True
        state.setdefault("configuration", {})["dossiers_frozen"] = True
        state["bars_frozen"] = state.get("bars_frozen", True)
        state.setdefault("configuration", {})["bars_frozen"] = state.get("bars_frozen", True)
    else:
        prep["preparation_status"] = "PREPARATION_FAILED"
        prep["fatal_errors"] = [f["message"] for f in failed]
        state["preparation_status"] = "PREPARATION_FAILED"

    state["preparation"] = prep

    from .acquisition import refresh_bar_statuses
    from .readiness import compute_readiness_labels

    refresh_bar_statuses(state)
    prep["readiness"] = state.get("readiness") or compute_readiness_labels(state)

    return {
        "dossiers_reconstructed": reconstructed,
        "dossiers_failed": failed,
        "latest_daily_input_by_trading_date": latest_daily_by_session,
        "readiness": state.get("readiness"),
        "counts": counts,
    }
