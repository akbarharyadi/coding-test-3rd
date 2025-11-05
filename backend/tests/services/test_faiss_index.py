"""
Tests for FAISS index manager.
"""
from unittest.mock import MagicMock, patch

import numpy as np
import pytest


@pytest.fixture
def mock_db_session():
    """Mock database session."""
    return MagicMock()


def test_faiss_not_available():
    """Test behavior when FAISS is not installed."""
    with patch("app.services.faiss_index.FAISS_AVAILABLE", False):
        from app.services.faiss_index import FaissIndexManager

        with pytest.raises(RuntimeError, match="FAISS is not installed"):
            FaissIndexManager()


@patch("app.services.faiss_index.FAISS_AVAILABLE", True)
@patch("app.services.faiss_index.faiss")
def test_append_embeddings(mock_faiss, mock_db_session, tmp_path):
    """Test appending embeddings to FAISS index."""
    from app.services.faiss_index import FaissIndexManager

    # Mock FAISS index
    mock_index = MagicMock()
    mock_faiss.IndexFlatIP.return_value = mock_index
    mock_faiss.read_index.return_value = mock_index

    with patch("app.core.config.settings") as mock_settings:
        mock_settings.OPENAI_API_KEY = None
        mock_settings.OLLAMA_BASE_URL = "http://localhost:11434"
        mock_settings.OLLAMA_EMBED_DIMENSION = 384
        mock_settings.VECTOR_STORE_PATH = str(tmp_path)

        manager = FaissIndexManager(db=mock_db_session)

        # Create test embeddings
        embeddings = [np.random.rand(384).astype("float32") for _ in range(3)]
        metadata = [{"document_id": i, "fund_id": 100} for i in range(3)]

        manager.append_embeddings(embeddings, metadata)

        # Verify index.add was called
        assert mock_index.add.called


@patch("app.services.faiss_index.FAISS_AVAILABLE", True)
@patch("app.services.faiss_index.faiss")
def test_rebuild_from_database(mock_faiss, mock_db_session, tmp_path):
    """Test rebuilding FAISS index from database."""
    from app.services.faiss_index import FaissIndexManager

    # Mock FAISS index
    mock_index = MagicMock()
    mock_index.ntotal = 3
    mock_faiss.IndexFlatIP.return_value = mock_index

    # Mock database results
    mock_db_session.execute.return_value.fetchall.return_value = [
        ("[0.1, 0.2, 0.3]", '{"document_id": 1, "fund_id": 100}'),
        ("[0.4, 0.5, 0.6]", '{"document_id": 2, "fund_id": 100}'),
        ("[0.7, 0.8, 0.9]", '{"document_id": 3, "fund_id": 100}'),
    ]

    with patch("app.core.config.settings") as mock_settings:
        mock_settings.OPENAI_API_KEY = None
        mock_settings.OLLAMA_BASE_URL = "http://localhost:11434"
        mock_settings.OLLAMA_EMBED_DIMENSION = 3  # Use small dimension for testing
        mock_settings.VECTOR_STORE_PATH = str(tmp_path)

        manager = FaissIndexManager(db=mock_db_session)
        count = manager.rebuild_from_database()

        assert count == 3
        assert mock_index.add.called
        assert mock_faiss.write_index.called


@patch("app.services.faiss_index.FAISS_AVAILABLE", True)
@patch("app.services.faiss_index.faiss")
def test_rebuild_from_database_empty(mock_faiss, mock_db_session, tmp_path):
    """Test rebuilding FAISS index when database is empty."""
    from app.services.faiss_index import FaissIndexManager

    # Mock empty database results
    mock_db_session.execute.return_value.fetchall.return_value = []

    with patch("app.core.config.settings") as mock_settings:
        mock_settings.OPENAI_API_KEY = None
        mock_settings.OLLAMA_BASE_URL = "http://localhost:11434"
        mock_settings.OLLAMA_EMBED_DIMENSION = 384
        mock_settings.VECTOR_STORE_PATH = str(tmp_path)

        manager = FaissIndexManager(db=mock_db_session)
        count = manager.rebuild_from_database()

        assert count == 0


@patch("app.services.faiss_index.FAISS_AVAILABLE", True)
@patch("app.services.faiss_index.faiss")
def test_search(mock_faiss, mock_db_session, tmp_path):
    """Test FAISS search functionality."""
    from app.services.faiss_index import FaissIndexManager

    # Mock FAISS index
    mock_index = MagicMock()
    mock_index.ntotal = 3
    # Mock search results: distances and indices
    mock_index.search.return_value = (
        np.array([[0.95, 0.85, 0.75]]),  # distances (similarity scores)
        np.array([[0, 1, 2]]),  # indices
    )
    mock_faiss.read_index.return_value = mock_index

    with patch("app.core.config.settings") as mock_settings:
        mock_settings.OPENAI_API_KEY = None
        mock_settings.OLLAMA_BASE_URL = "http://localhost:11434"
        mock_settings.OLLAMA_EMBED_DIMENSION = 384
        mock_settings.VECTOR_STORE_PATH = str(tmp_path)

        manager = FaissIndexManager(db=mock_db_session)

        # Create metadata file
        metadata = [
            {"document_id": 1, "fund_id": 100},
            {"document_id": 2, "fund_id": 100},
            {"document_id": 3, "fund_id": 200},
        ]
        manager._write_metadata(metadata)

        # Create dummy index file
        manager.index_path.touch()

        # Perform search
        query_embedding = np.random.rand(384).astype("float32")
        results = manager.search(query_embedding, k=2, fund_id=100)

        assert len(results) == 2
        assert results[0]["metadata"]["fund_id"] == 100
        assert results[1]["metadata"]["fund_id"] == 100


@patch("app.services.faiss_index.FAISS_AVAILABLE", True)
@patch("app.services.faiss_index.faiss")
def test_search_nonexistent_index(mock_faiss, mock_db_session, tmp_path):
    """Test search when index doesn't exist."""
    from app.services.faiss_index import FaissIndexManager

    with patch("app.core.config.settings") as mock_settings:
        mock_settings.OPENAI_API_KEY = None
        mock_settings.OLLAMA_BASE_URL = "http://localhost:11434"
        mock_settings.OLLAMA_EMBED_DIMENSION = 384
        mock_settings.VECTOR_STORE_PATH = str(tmp_path)

        manager = FaissIndexManager(db=mock_db_session)

        # Perform search on nonexistent index
        query_embedding = np.random.rand(384).astype("float32")
        results = manager.search(query_embedding, k=5)

        assert len(results) == 0


def test_normalize():
    """Test vector normalization."""
    from app.services.faiss_index import FaissIndexManager

    # Test normal vector
    vector = np.array([3.0, 4.0])
    normalized = FaissIndexManager._normalize(vector)
    assert np.isclose(np.linalg.norm(normalized), 1.0)

    # Test zero vector
    zero_vector = np.array([0.0, 0.0])
    normalized_zero = FaissIndexManager._normalize(zero_vector)
    assert np.allclose(normalized_zero, zero_vector)
