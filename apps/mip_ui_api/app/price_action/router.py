import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from .models import AnalyseRequest, AnalyseResponse
from .service import PriceActionError, analyse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/price-action-analyser", tags=["price-action-analyser"])


@router.post("/analyse", response_model=AnalyseResponse)
def analyse_price_action(request: AnalyseRequest) -> AnalyseResponse:
    """Analyse canonical daily bars without creating or changing trading state."""
    if request.side != "LONG":
        return JSONResponse(
            status_code=400,
            content={
                "status": "UNSUPPORTED_SIDE",
                "message": "Price Action Analyser v1 supports LONG analysis only.",
            },
        )
    try:
        return analyse(request)
    except PriceActionError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception(
            "price_action event=request_failed symbol=%s error_class=%s",
            request.symbol,
            type(exc).__name__,
        )
        raise HTTPException(status_code=500, detail="Price action analysis failed.") from exc
