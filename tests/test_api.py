"""
REST API tests against the real database (read-only: nothing is written).
"""

import pytest
from fastapi.testclient import TestClient

from src.api.dependencies import settings_dependency
from src.api.main import app

pytestmark = pytest.mark.db


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["database"] == "ok"


def test_summary_shape_and_money_as_numbers(client):
    body = client.get("/api/v1/claims/summary").json()
    assert set(body) >= {"total_claims", "total_billed", "approval_rate_percent", "possible_duplicates"}
    assert isinstance(body["total_billed"], (int, float))


def test_filters_narrow_the_summary(client):
    total = client.get("/api/v1/claims/summary").json()["total_claims"]
    medical = client.get("/api/v1/claims/summary", params={"claim_type": "Medical"}).json()["total_claims"]
    assert medical <= total


def test_reversed_date_range_is_rejected(client):
    response = client.get(
        "/api/v1/claims/summary",
        params={"service_date_from": "2026-01-01", "service_date_to": "2025-01-01"},
    )
    assert response.status_code == 422


def test_invalid_order_by_is_rejected(client):
    assert client.get("/api/v1/providers/top", params={"order_by": "DROP TABLE"}).status_code == 422


def test_page_size_is_capped(client, settings):
    body = client.get("/api/v1/claims/duplicates", params={"limit": 1_000_000}).json()
    assert body["limit"] == settings.api_max_page_size


def test_unknown_claim_is_404(client):
    assert client.get("/api/v1/claims/DOES_NOT_EXIST").status_code == 404
    assert client.get("/api/v1/lineage/claims/DOES_NOT_EXIST").status_code == 404


def test_rules_endpoint_lists_configured_rules(client):
    codes = {rule["code"] for rule in client.get("/api/v1/quality/rules").json()}
    assert "MISSING_DENIAL_REASON" in codes


def test_api_key_is_enforced_when_configured(client, settings):
    settings.api_key = "s3cret"
    app.dependency_overrides[settings_dependency] = lambda: settings

    assert client.get("/api/v1/claims/summary").status_code == 401
    assert client.get("/api/v1/claims/summary", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/api/v1/claims/summary", headers={"X-API-Key": "s3cret"}).status_code == 200
    # Health stays open for load balancers.
    assert client.get("/health").status_code == 200
