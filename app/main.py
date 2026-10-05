from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from app.api import router
from app.db import connect, disconnect
from app.request_log import RequestLogMiddleware
from app.settings import load_settings


@asynccontextmanager
async def lifespan(_app: FastAPI):
    settings = load_settings()
    await connect(settings.database_url)
    yield
    await disconnect()


app = FastAPI(title="Seat Reservation", lifespan=lifespan)
app.include_router(router)
app.add_middleware(RequestLogMiddleware)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    if isinstance(exc.detail, dict):
        error = exc.detail.get("error")
        if isinstance(error, str) and error:
            request.state.outcome = error
        return JSONResponse(status_code=exc.status_code, content=exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": "http_error", "message": str(exc.detail)},
    )
