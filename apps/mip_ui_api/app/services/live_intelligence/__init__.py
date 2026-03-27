"""Live Intelligence Cockpit — submodules load on demand to avoid import-time DB deps."""

__all__ = ["build_bootstrap_payload", "run_deterministic_step"]


def __getattr__(name: str):
    if name == "build_bootstrap_payload":
        from app.services.live_intelligence.bootstrap import build_bootstrap_payload

        return build_bootstrap_payload
    if name == "run_deterministic_step":
        from app.services.live_intelligence.engine import run_deterministic_step

        return run_deterministic_step
    raise AttributeError(name)
