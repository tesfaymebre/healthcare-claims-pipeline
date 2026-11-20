"""
Shared FastAPI dependencies: database session, auth, filters, paging.
"""

from __future__ import annotations

import secrets
from collections.abc import Iterator
from datetime import date
from typing import Annotated

from fastapi import Depends, HTTPException, Query, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.engine import Connection

from ..analytics import ClaimFilters
from ..config import Settings, get_settings
from ..database import get_engine


def settings_dependency() -> Settings:
    return get_settings()


SettingsDep = Annotated[Settings, Depends(settings_dependency)]


def get_connection() -> Iterator[Connection]:
    """
    One read-only connection per request, returned to the pool afterwards.

    postgresql_readonly makes PostgreSQL itself refuse any write, so a bug in
    a query can never modify claims data through the API.
    """

    with get_engine().connect() as connection:
        connection = connection.execution_options(postgresql_readonly=True)
        yield connection


ConnectionDep = Annotated[Connection, Depends(get_connection)]


_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(
    settings: SettingsDep,
    api_key: Annotated[str | None, Security(_api_key_header)],
) -> None:
    """
    Enforce X-API-Key when API_KEY is set in the environment.

    With no API_KEY configured the API is open, which is only appropriate on
    a developer machine; the startup log says so. compare_digest avoids
    leaking the key through response timing.
    """

    expected = settings.api_key

    if expected is None:
        return

    if api_key is None or not secrets.compare_digest(api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API key",
            headers={"WWW-Authenticate": "APIKey"},
        )


def claim_filters(
    service_date_from: Annotated[date | None, Query(description="Inclusive lower bound on service_date")] = None,
    service_date_to: Annotated[date | None, Query(description="Inclusive upper bound on service_date")] = None,
    claim_type: Annotated[str | None, Query(max_length=30)] = None,
    insurance_plan: Annotated[str | None, Query(max_length=50)] = None,
    claim_status: Annotated[str | None, Query(max_length=20)] = None,
    provider_id: Annotated[str | None, Query(max_length=50)] = None,
) -> ClaimFilters:
    if service_date_from and service_date_to and service_date_from > service_date_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="service_date_from must not be after service_date_to",
        )
    return ClaimFilters(
        service_date_from=service_date_from,
        service_date_to=service_date_to,
        claim_type=claim_type,
        insurance_plan=insurance_plan,
        claim_status=claim_status,
        provider_id=provider_id,
    )


FiltersDep = Annotated[ClaimFilters, Depends(claim_filters)]


class Paging:
    def __init__(self, limit: int, offset: int) -> None:
        self.limit = limit
        self.offset = offset


def paging(
    settings: SettingsDep,
    limit: Annotated[int | None, Query(ge=1, description="Page size (capped by api.max_page_size)")] = None,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> Paging:
    size = limit or settings.api_default_page_size
    return Paging(limit=min(size, settings.api_max_page_size), offset=offset)


PagingDep = Annotated[Paging, Depends(paging)]
