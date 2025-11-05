"""
Tests for semantic search API endpoints.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.search_service import SearchBackend

client = TestClient(app)


@pytest.fixture
def mock_search_service():
    """Mock search service."""
    service = MagicMock()
    service.search = AsyncMock(return_value=[
        {
            "content": "Capital call of $1,000,000",
            "metadata": {"document_id": 123, "fund_id": 456},
            "score": 0.89,
            "source": "postgresql",
        }
    ])
    service.prefer_backend = SearchBackend.POSTGRESQL
    service.get_stats = MagicMock(return_value={
        "available_backends": ["postgresql"],
        "preferred_backend": "postgresql",
        "faiss_available": False,
    })
    return service


def test_semantic_search_post(mock_search_service):
    """Test POST semantic search endpoint."""
    with patch("app.api.endpoints.search.SearchService", return_value=mock_search_service):
        response = client.post(
            "/api/search/",
            json={
                "query": "capital call",
                "k": 5,
                "fund_id": 456,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["query"] == "capital call"
        assert len(data["results"]) == 1
        assert data["results"][0]["content"] == "Capital call of $1,000,000"
        assert data["results"][0]["score"] == 0.89
        assert "processing_time" in data


def test_semantic_search_get(mock_search_service):
    """Test GET semantic search endpoint."""
    with patch("app.api.endpoints.search.SearchService", return_value=mock_search_service):
        response = client.get(
            "/api/search/",
            params={
                "query": "capital call",
                "k": 5,
                "fund_id": 456,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["query"] == "capital call"


def test_semantic_search_empty_query():
    """Test search with empty query."""
    response = client.post(
        "/api/search/",
        json={
            "query": "",
            "k": 5,
        },
    )

    assert response.status_code == 422  # Validation error


def test_semantic_search_invalid_k():
    """Test search with invalid k parameter."""
    response = client.post(
        "/api/search/",
        json={
            "query": "capital call",
            "k": 0,  # Invalid: must be >= 1
        },
    )

    assert response.status_code == 422  # Validation error


def test_semantic_search_with_backend_selection(mock_search_service):
    """Test search with specific backend selection."""
    with patch("app.api.endpoints.search.SearchService", return_value=mock_search_service):
        response = client.post(
            "/api/search/",
            json={
                "query": "capital call",
                "k": 5,
                "backend": "postgresql",
            },
        )

        assert response.status_code == 200


def test_semantic_search_without_content(mock_search_service):
    """Test search with include_content=False."""
    mock_search_service.search = AsyncMock(return_value=[
        {
            "metadata": {"document_id": 123, "fund_id": 456},
            "score": 0.89,
            "source": "postgresql",
        }
    ])

    with patch("app.api.endpoints.search.SearchService", return_value=mock_search_service):
        response = client.post(
            "/api/search/",
            json={
                "query": "capital call",
                "k": 5,
                "include_content": False,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["results"][0].get("content") is None


def test_get_search_stats(mock_search_service):
    """Test search statistics endpoint."""
    with patch("app.api.endpoints.search.SearchService", return_value=mock_search_service):
        response = client.get("/api/search/stats")

        assert response.status_code == 200
        data = response.json()
        assert "available_backends" in data
        assert "preferred_backend" in data
        assert "faiss_available" in data
        assert data["postgresql_available"] is True


@patch("app.services.faiss_index.FAISS_AVAILABLE", True)
@patch("app.services.faiss_index.FaissIndexManager")
def test_rebuild_faiss_index(mock_faiss_manager_class):
    """Test FAISS index rebuild endpoint."""
    mock_manager = MagicMock()
    mock_manager.rebuild_from_database.return_value = 1234
    mock_faiss_manager_class.return_value = mock_manager

    response = client.post("/api/search/rebuild-index")

    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "FAISS index rebuilt successfully"
    assert data["vectors_indexed"] == 1234
    assert data["fund_id"] is None


@patch("app.services.faiss_index.FAISS_AVAILABLE", True)
@patch("app.services.faiss_index.FaissIndexManager")
def test_rebuild_faiss_index_with_fund_filter(mock_faiss_manager_class):
    """Test FAISS index rebuild with fund_id filter."""
    mock_manager = MagicMock()
    mock_manager.rebuild_from_database.return_value = 500
    mock_faiss_manager_class.return_value = mock_manager

    response = client.post("/api/search/rebuild-index?fund_id=123")

    assert response.status_code == 200
    data = response.json()
    assert data["vectors_indexed"] == 500
    assert data["fund_id"] == 123


@patch("app.services.faiss_index.FAISS_AVAILABLE", False)
def test_rebuild_faiss_index_not_available():
    """Test rebuild endpoint when FAISS is not available."""
    response = client.post("/api/search/rebuild-index")

    assert response.status_code == 503
    assert "FAISS is not available" in response.json()["detail"]


def test_search_with_document_filter(mock_search_service):
    """Test search with document_id filter."""
    with patch("app.api.endpoints.search.SearchService", return_value=mock_search_service):
        response = client.post(
            "/api/search/",
            json={
                "query": "capital call",
                "k": 5,
                "document_id": 123,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1


def test_search_error_handling():
    """Test search error handling."""
    with patch("app.api.endpoints.search.SearchService") as mock_service_class:
        mock_service = MagicMock()
        mock_service.search = AsyncMock(side_effect=Exception("Database error"))
        mock_service_class.return_value = mock_service

        response = client.post(
            "/api/search/",
            json={
                "query": "capital call",
                "k": 5,
            },
        )

        assert response.status_code == 500
        assert "Search failed" in response.json()["detail"]
