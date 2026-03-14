from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/portfolios", tags=["portfolios"])

DEPRECATION_MESSAGE = (
    "Sim portfolio endpoints are retired. Use live endpoints under /live "
    "(for example /live/portfolio-config, /live/metrics, /live/activity)."
)


def _retired(path: str):
    raise HTTPException(
        status_code=410,
        detail={
            "status": "DEPRECATED",
            "path": path,
            "message": DEPRECATION_MESSAGE,
        },
    )


@router.get("")
def retired_portfolios_root():
    _retired("/portfolios")


@router.api_route("/{subpath:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def retired_portfolios_subpaths(subpath: str, request: Request):
    _retired(f"/portfolios/{subpath} [{request.method}]")
