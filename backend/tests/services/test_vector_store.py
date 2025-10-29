from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest

from app.services.vector_store import VectorStore


@pytest.mark.asyncio
async def test_add_document_inserts_vector(monkeypatch):
    mock_session = MagicMock()
    vector_store = object.__new__(VectorStore)
    vector_store.db = mock_session
    vector_store.embeddings = MagicMock()
    vector_store._get_embedding = AsyncMock(return_value=np.array([0.1, 0.2], dtype=np.float32))

    await vector_store.add_document(
        "sample content",
        {"document_id": 5, "fund_id": 2, "page_number": 1},
    )

    mock_session.execute.assert_called_once()
    params = mock_session.execute.call_args[0][1]
    assert params["document_id"] == 5
    assert params["fund_id"] == 2
    assert params["metadata"] is not None
    assert params["embedding"].startswith("[") and params["embedding"].endswith("]")
    mock_session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_similarity_search_formats_results(monkeypatch):
    mock_session = MagicMock()
    mock_session.execute.return_value = [
        (1, 5, 2, "content", '{"key": "value"}', 0.95),
    ]

    vector_store = object.__new__(VectorStore)
    vector_store.db = mock_session
    vector_store.embeddings = MagicMock()
    vector_store._get_embedding = AsyncMock(return_value=np.array([0.3, 0.4], dtype=np.float32))

    results = await vector_store.similarity_search("query", k=1, filter_metadata={"fund_id": 2})

    assert len(results) == 1
    assert results[0]["document_id"] == 5
    assert results[0]["metadata"] == {"key": "value"}
    mock_session.execute.assert_called_once()
