"""
FAISS index manager for semantic search.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import SessionLocal

try:  # pragma: no cover - optional dependency
    import faiss  # type: ignore

    FAISS_AVAILABLE = True
    from faiss import Index as FaissIndex
except ImportError:  # pragma: no cover
    faiss = None  # type: ignore
    FAISS_AVAILABLE = False
    FaissIndex = Any

logger = logging.getLogger(__name__)


class FaissIndexManager:
    """Manage FAISS index files for fast vector search."""

    def __init__(self, db: Optional[Session] = None):
        if not FAISS_AVAILABLE:  # pragma: no cover - runtime guard
            raise RuntimeError(
                "FAISS is not installed. Install faiss-cpu to enable FAISS index management."
            )

        self.db = db or SessionLocal()
        self.dimension = (
            1536
            if settings.OPENAI_API_KEY
            else settings.OLLAMA_EMBED_DIMENSION if settings.OLLAMA_BASE_URL else 384
        )
        self.index_dir = Path(settings.VECTOR_STORE_PATH).resolve()
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.index_path = self.index_dir / "documents.faiss"
        self.metadata_path = self.index_dir / "documents_metadata.json"

    # ------------------------------------------------------------------ #
    # High level operations
    # ------------------------------------------------------------------ #
    def append_embeddings(
        self,
        embeddings: Iterable[np.ndarray],
        metadata: Iterable[Dict[str, Any]],
    ) -> None:
        """Append new embeddings and metadata to the FAISS index."""
        if not FAISS_AVAILABLE:  # pragma: no cover - ensures proper usage
            raise RuntimeError(
                "FAISS is not installed. Install faiss-cpu to enable this feature."
            )

        embeddings = list(embeddings)
        metadata = list(metadata)
        if not embeddings:
            return

        index = self._load_index()
        metadata_list = self._load_metadata()

        vectors = np.stack([self._normalize(e) for e in embeddings]).astype("float32")
        index.add(vectors)
        faiss.write_index(index, str(self.index_path))

        metadata_list.extend(metadata)
        self._write_metadata(metadata_list)

    def rebuild_from_database(self, fund_id: Optional[int] = None) -> int:
        """Rebuild the index from all stored embeddings."""
        if not FAISS_AVAILABLE:  # pragma: no cover - ensures proper usage
            raise RuntimeError(
                "FAISS is not installed. Install faiss-cpu to enable this feature."
            )

        query = "SELECT embedding, metadata FROM document_embeddings"
        params: Dict[str, Any] = {}
        if fund_id is not None:
            query += " WHERE fund_id = :fund_id"
            params["fund_id"] = fund_id

        rows = self.db.execute(text(query), params).fetchall()
        if not rows:
            self._clear_files()
            logger.info("No embeddings found to rebuild FAISS index.")
            return 0

        embeddings: List[np.ndarray] = []
        metadata: List[Dict[str, Any]] = []
        for embedding_str, metadata_str in rows:
            try:
                embeddings.append(np.array(json.loads(embedding_str), dtype="float32"))
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Failed to parse embedding: %s", exc)
                continue

            if isinstance(metadata_str, dict):
                metadata.append(metadata_str)
            else:
                try:
                    metadata.append(json.loads(metadata_str))
                except Exception:
                    metadata.append({"raw": metadata_str})

        index = faiss.IndexFlatIP(self.dimension)
        index.add(np.stack([self._normalize(e) for e in embeddings]))
        faiss.write_index(index, str(self.index_path))
        self._write_metadata(metadata)

        logger.info("Rebuilt FAISS index with %s vectors.", index.ntotal)
        return index.ntotal

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _load_index(self) -> FaissIndex:
        if not FAISS_AVAILABLE:  # pragma: no cover - ensures proper usage
            raise RuntimeError(
                "FAISS is not installed. Install faiss-cpu to enable this feature."
            )

        if self.index_path.exists():
            return faiss.read_index(str(self.index_path))
        return faiss.IndexFlatIP(self.dimension)

    def _load_metadata(self) -> List[Dict[str, Any]]:
        if self.metadata_path.exists():
            try:
                return json.loads(self.metadata_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                logger.warning("Failed to parse FAISS metadata file. Starting fresh.")
        return []

    def _write_metadata(self, metadata: List[Dict[str, Any]]) -> None:
        self.metadata_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _clear_files(self) -> None:
        if self.index_path.exists():
            os.remove(self.index_path)
        if self.metadata_path.exists():
            os.remove(self.metadata_path)

    @staticmethod
    def _normalize(vector: np.ndarray) -> np.ndarray:
        norm = np.linalg.norm(vector)
        if norm == 0.0:
            return vector
        return vector / norm
