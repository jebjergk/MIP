from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/decisions", tags=["decisions"])

DEPRECATION_MESSAGE = (
    "Intraday early-exit decision endpoints are retired. "
    "Use /live/decisions for current execution workflow."
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
def retired_decisions_root():
    _retired("/decisions")


@router.api_route("/{subpath:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def retired_decisions_subpaths(subpath: str, request: Request):
    _retired(f"/decisions/{subpath} [{request.method}]")
