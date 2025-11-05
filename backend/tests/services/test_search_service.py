"""
Tests for the semantic search service.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from app.services.search_service import SearchBackend, SearchService


@pytest.fixture
def mock_db_session():
    """Mock database session."""
    return MagicMock()


@pytest.fixture
def mock_vector_store():
    """Mock vector store."""
    store = MagicMock()
    store.similarity_search = AsyncMock(return_value=[
        {
            "content": "Capital call document",
            "metadata": {"document_id": 1, "fund_id": 100},
            "score": 0.95,
        }
    ])
    store._get_embedding = AsyncMock(return_value=np.random.rand(384).astype("float32"))
    return store


@pytest.fixture
def mock_faiss_manager():
    """Mock FAISS manager."""
    manager = MagicMock()
    manager.search = MagicMock(return_value=[
        {
            "metadata": {"document_id": 1, "fund_id": 100, "offset_start": 0},
            "score": 0.92,
            "index": 0,
        }
    ])
    manager.index_path.exists.return_value = True
    return manager


@pytest.mark.asyncio
async def test_postgresql_search(mock_db_session, mock_vector_store):
    """Test PostgreSQL search backend."""
    with patch("app.services.search_service.VectorStore", return_value=mock_vector_store):
        with patch("app.services.search_service.FAISS_AVAILABLE", False):
            service = SearchService(db=mock_db_session)

            results = await service.search(
                query="capital call",
                k=5,
                fund_id=100,
            )

            assert len(results) == 1
            assert results[0]["content"] == "Capital call document"
            assert results[0]["source"] == "postgresql"
            assert results[0]["score"] == 0.95


@pytest.mark.asyncio
async def test_faiss_search(mock_db_session, mock_vector_store, mock_faiss_manager):
    """Test FAISS search backend."""
    with patch("app.services.search_service.VectorStore", return_value=mock_vector_store):
        with patch("app.services.search_service.FAISS_AVAILABLE", True):
            with patch("app.services.search_service.FaissIndexManager", return_value=mock_faiss_manager):
                service = SearchService(db=mock_db_session, prefer_backend=SearchBackend.FAISS)

                # Mock the content fetch
                service._fetch_content_from_db = AsyncMock(return_value="Capital call document")

                results = await service.search(
                    query="capital call",
                    k=5,
                    fund_id=100,
                    backend=SearchBackend.FAISS,
                )

                assert len(results) == 1
                assert results[0]["source"] == "faiss"
                assert results[0]["score"] == 0.92


@pytest.mark.asyncio
async def test_hybrid_search(mock_db_session, mock_vector_store, mock_faiss_manager):
    """Test hybrid search combining both backends."""
    with patch("app.services.search_service.VectorStore", return_value=mock_vector_store):
        with patch("app.services.search_service.FAISS_AVAILABLE", True):
            with patch("app.services.search_service.FaissIndexManager", return_value=mock_faiss_manager):
                service = SearchService(db=mock_db_session)
                service._fetch_content_from_db = AsyncMock(return_value="Capital call document")

                results = await service.search(
                    query="capital call",
                    k=5,
                    backend=SearchBackend.HYBRID,
                )

                # Should have results from both backends (deduplicated)
                assert len(results) >= 1


@pytest.mark.asyncio
async def test_search_with_empty_query(mock_db_session):
    """Test that empty query raises ValueError."""
    with patch("app.services.search_service.FAISS_AVAILABLE", False):
        service = SearchService(db=mock_db_session)

        with pytest.raises(ValueError, match="Query must be a non-empty string"):
            await service.search(query="", k=5)


@pytest.mark.asyncio
async def test_search_with_invalid_k(mock_db_session):
    """Test that invalid k raises ValueError."""
    with patch("app.services.search_service.FAISS_AVAILABLE", False):
        service = SearchService(db=mock_db_session)

        with pytest.raises(ValueError, match="k must be a positive integer"):
            await service.search(query="test", k=0)


@pytest.mark.asyncio
async def test_search_without_content(mock_db_session, mock_vector_store):
    """Test search with include_content=False."""
    with patch("app.services.search_service.VectorStore", return_value=mock_vector_store):
        with patch("app.services.search_service.FAISS_AVAILABLE", False):
            service = SearchService(db=mock_db_session)

            results = await service.search(
                query="capital call",
                k=5,
                include_content=False,
            )

            assert len(results) == 1
            assert "content" not in results[0]


@pytest.mark.asyncio
async def test_faiss_fallback_to_postgresql(mock_db_session, mock_vector_store):
    """Test FAISS fallback to PostgreSQL when FAISS unavailable."""
    with patch("app.services.search_service.VectorStore", return_value=mock_vector_store):
        with patch("app.services.search_service.FAISS_AVAILABLE", False):
            service = SearchService(db=mock_db_session)

            # Request FAISS but should fallback to PostgreSQL
            results = await service.search(
                query="capital call",
                k=5,
                backend=SearchBackend.FAISS,
            )

            assert len(results) == 1
            assert results[0]["source"] == "postgresql"


def test_get_stats_postgresql_only(mock_db_session):
    """Test stats when only PostgreSQL is available."""
    with patch("app.services.search_service.FAISS_AVAILABLE", False):
        service = SearchService(db=mock_db_session)
        stats = service.get_stats()

        assert stats["available_backends"] == ["postgresql"]
        assert stats["preferred_backend"] == "postgresql"
        assert stats["faiss_available"] is False


def test_get_stats_with_faiss(mock_db_session, mock_faiss_manager):
    """Test stats when FAISS is available."""
    mock_index = MagicMock()
    mock_index.ntotal = 1234
    mock_faiss_manager._load_index.return_value = mock_index

    with patch("app.services.search_service.FAISS_AVAILABLE", True):
        with patch("app.services.search_service.FaissIndexManager", return_value=mock_faiss_manager):
            service = SearchService(db=mock_db_session)
            stats = service.get_stats()

            assert "faiss" in stats["available_backends"]
            assert "hybrid" in stats["available_backends"]
            assert stats["faiss_available"] is True
            assert stats["faiss_vectors"] == 1234


def test_auto_backend_selection_prefers_faiss(mock_db_session, mock_faiss_manager):
    """Test that auto selection prefers FAISS when available."""
    with patch("app.services.search_service.FAISS_AVAILABLE", True):
        with patch("app.services.search_service.FaissIndexManager", return_value=mock_faiss_manager):
            service = SearchService(db=mock_db_session)

            assert service.prefer_backend == SearchBackend.FAISS


def test_auto_backend_selection_falls_back_to_postgresql(mock_db_session):
    """Test that auto selection falls back to PostgreSQL."""
    with patch("app.services.search_service.FAISS_AVAILABLE", False):
        service = SearchService(db=mock_db_session)

        assert service.prefer_backend == SearchBackend.POSTGRESQL
