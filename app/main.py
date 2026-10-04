from fastapi import FastAPI

from app.api import router

app = FastAPI(title="Seat Reservation")
app.include_router(router)
