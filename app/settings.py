import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    port: int
    database_url: str
    auth_secret: str
    admin_token: str


def load_settings() -> Settings:
    return Settings(
        port=int(os.getenv("PORT", "8000")),
        database_url=os.getenv(
            "DATABASE_URL",
            "postgresql://seat:seat@localhost:5432/seat",
        ),
        auth_secret=os.getenv("AUTH_SECRET", ""),
        admin_token=os.getenv("ADMIN_TOKEN", ""),
    )
