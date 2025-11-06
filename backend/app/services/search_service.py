"""
Unified semantic search service that orchestrates PostgreSQL and FAISS vector stores.

This service provides a high-level interface for semantic search across document embeddings,
intelligently selecting between FAISS (fast in-memory search) and PostgreSQL (with pgvector)
based on availability and requirements.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from enum import Enum

import numpy as np
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import SessionLocal
from app.services.vector_store import VectorStore
from app.services.faiss_index import FAISS_AVAILABLE, FaissIndexManager

logger = logging.getLogger(__name__)


class SearchBackend(str, Enum):
    """Available search backends."""

    POSTGRESQL = "postgresql"
    FAISS = "faiss"
    HYBRID = "hybrid"


class SearchService:
    """
    Unified semantic search service with multiple backend support.

    This service provides semantic search capabilities across document embeddings,
    intelligently routing queries to the most appropriate backend (PostgreSQL or FAISS)
    based on availability, dataset size, and query requirements.

    Features:
    - Automatic backend selection (FAISS for speed, PostgreSQL for filtering)
    - Hybrid search combining results from multiple backends
    - Support for metadata filtering
    - Consistent result format across backends

    Args:
        db: Optional SQLAlchemy session
        prefer_backend: Preferred search backend (auto-selected if None)

    Example:
        >>> search = SearchService()
        >>> import asyncio
        >>> async def search_example():
        ...     results = await search.search(
        ...         query="capital call distribution",
        ...         k=5,
        ...         fund_id=123
        ...     )
        ...     return len(results) > 0
    """

    def __init__(
        self,
        db: Optional[Session] = None,
        prefer_backend: Optional[SearchBackend] = None,
    ):
        self.db = db or SessionLocal()
        self.vector_store = VectorStore(db=self.db)
        self.faiss_available = FAISS_AVAILABLE
        self.faiss_manager: Optional[FaissIndexManager] = None

        if self.faiss_available:
            try:
                self.faiss_manager = FaissIndexManager(db=self.db)
            except Exception as exc:  # pragma: no cover - optional feature
                logger.warning("Failed to initialize FAISS manager: %s", exc)
                self.faiss_available = False

        self.prefer_backend = prefer_backend or self._auto_select_backend()
        logger.info("SearchService initialized with backend: %s", self.prefer_backend)

    def _auto_select_backend(self) -> SearchBackend:
        """
        Automatically select the best search backend.

        Selection logic:
        - If FAISS is available and has data, prefer FAISS for speed
        - Otherwise, use PostgreSQL
        - For complex filtering needs, use hybrid approach
        """
        if self.faiss_available and self.faiss_manager:
            if self.faiss_manager.index_path.exists():
                return SearchBackend.FAISS

        return SearchBackend.POSTGRESQL

    async def search(
        self,
        query: str,
        k: int = 5,
        fund_id: Optional[int] = None,
        document_id: Optional[int] = None,
        backend: Optional[SearchBackend] = None,
        include_content: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Perform semantic search across document embeddings.

        This method generates an embedding for the query and searches for the most
        similar documents using the specified or auto-selected backend.

        Args:
            query: Search query text
            k: Number of results to return (default: 5)
            fund_id: Optional fund ID filter
            document_id: Optional document ID filter
            backend: Optional backend selection (uses default if None)
            include_content: Whether to include full content in results (default: True)

        Returns:
            List of search results with metadata and similarity scores. Each result contains:
            - content: Document text (if include_content=True)
            - metadata: Document metadata
            - score: Similarity score (higher = more similar)
            - source: Which backend provided this result

        Example:
            >>> search = SearchService()
            >>> import asyncio
            >>> results = asyncio.run(
            ...     search.search("capital call", k=3, fund_id=123)
            ... )
        """
        if not query or not query.strip():
            raise ValueError("Query must be a non-empty string")

        if k <= 0:
            raise ValueError("k must be a positive integer")

        # Use specified backend or fall back to preference
        selected_backend = backend or self.prefer_backend

        logger.info(
            "Performing semantic search: query='%s', k=%s, fund_id=%s, backend=%s",
            query[:50],
            k,
            fund_id,
            selected_backend,
        )

        try:
            if selected_backend == SearchBackend.HYBRID:
                return await self._hybrid_search(
                    query=query,
                    k=k,
                    fund_id=fund_id,
                    document_id=document_id,
                    include_content=include_content,
                )
            elif selected_backend == SearchBackend.FAISS:
                return await self._faiss_search(
                    query=query,
                    k=k,
                    fund_id=fund_id,
                    document_id=document_id,
                    include_content=include_content,
                )
            else:  # PostgreSQL
                return await self._postgresql_search(
                    query=query,
                    k=k,
                    fund_id=fund_id,
                    document_id=document_id,
                    include_content=include_content,
                )
        except Exception as exc:
            logger.error("Search failed: %s", exc)
            raise

    async def _postgresql_search(
        self,
        query: str,
        k: int,
        fund_id: Optional[int],
        document_id: Optional[int],
        include_content: bool,
    ) -> List[Dict[str, Any]]:
        """Search using PostgreSQL pgvector."""
        filter_metadata = {}
        if fund_id is not None:
            filter_metadata["fund_id"] = fund_id
        if document_id is not None:
            filter_metadata["document_id"] = document_id

        results = await self.vector_store.similarity_search(
            query=query,
            k=k,
            filter_metadata=filter_metadata if filter_metadata else None,
        )

        # Add source information, document/fund names, and optionally remove content
        for result in results:
            result["source"] = "postgresql"

            # Fetch and add document and fund names
            doc_id = result.get("metadata", {}).get("document_id")
            if doc_id:
                doc_title, fund_name = await self._fetch_document_and_fund_names(doc_id)
                if doc_title:
                    result["metadata"]["document_title"] = doc_title
                if fund_name:
                    result["metadata"]["fund_name"] = fund_name

            if not include_content:
                result.pop("content", None)

        return results

    async def _faiss_search(
        self,
        query: str,
        k: int,
        fund_id: Optional[int],
        document_id: Optional[int],
        include_content: bool,
    ) -> List[Dict[str, Any]]:
        """Search using FAISS index."""
        if not self.faiss_available or not self.faiss_manager:
            logger.warning("FAISS not available, falling back to PostgreSQL")
            return await self._postgresql_search(
                query=query,
                k=k,
                fund_id=fund_id,
                document_id=document_id,
                include_content=include_content,
            )

        # Generate query embedding
        query_embedding = await self.vector_store._get_embedding(query)

        # Search FAISS index
        faiss_results = self.faiss_manager.search(
            query_embedding=query_embedding,
            k=k,
            fund_id=fund_id,
        )

        # If no results found with fund_id filter, retry without filter
        # and filter by fund name in the results
        if not faiss_results and fund_id is not None:
            from app.models.fund import Fund

            # Get the fund name for this fund_id
            fund = self.db.query(Fund).filter(Fund.id == fund_id).first()

            if fund and fund.name:
                logger.info(
                    "No FAISS results found for fund_id=%s, retrying without filter and matching fund name '%s'",
                    fund_id,
                    fund.name
                )
                # Search across all funds
                all_results = self.faiss_manager.search(
                    query_embedding=query_embedding,
                    k=k * 3,  # Get more results for filtering
                    fund_id=None,
                )

                # Filter results by fund name in content or metadata
                fund_name_lower = fund.name.lower()
                gp_name_lower = fund.gp_name.lower() if fund.gp_name else None

                for result in all_results:
                    metadata = result.get("metadata", {})

                    # Check if fund name or GP name appears in document name or metadata
                    doc_name = metadata.get("document_name", "").lower()
                    fund_name_meta = metadata.get("fund_name", "").lower()

                    if (fund_name_lower in doc_name or
                        fund_name_lower in fund_name_meta or
                        (gp_name_lower and gp_name_lower in doc_name)):
                        faiss_results.append(result)
                        if len(faiss_results) >= k:
                            break

                if faiss_results:
                    logger.info(
                        "Found %s results by matching fund name '%s' in document metadata",
                        len(faiss_results),
                        fund.name
                    )

            # If still no results, use top results regardless of fund
            if not faiss_results:
                logger.info("No fund-name matches found, returning top results")
                faiss_results = self.faiss_manager.search(
                    query_embedding=query_embedding,
                    k=k,
                    fund_id=None,
                )

        # Enrich results with content from database if needed
        if include_content or document_id is not None:
            faiss_results = await self._enrich_faiss_results(
                faiss_results,
                include_content=include_content,
                document_id_filter=document_id,
            )

        # Add source information
        for result in faiss_results:
            result["source"] = "faiss"

        return faiss_results

    async def _hybrid_search(
        self,
        query: str,
        k: int,
        fund_id: Optional[int],
        document_id: Optional[int],
        include_content: bool,
    ) -> List[Dict[str, Any]]:
        """
        Perform hybrid search combining PostgreSQL and FAISS results.

        This uses both backends and merges results, removing duplicates and
        re-ranking by score.
        """
        # Get results from both backends
        faiss_results = await self._faiss_search(
            query=query,
            k=k,
            fund_id=fund_id,
            document_id=document_id,
            include_content=include_content,
        )

        pg_results = await self._postgresql_search(
            query=query,
            k=k,
            fund_id=fund_id,
            document_id=document_id,
            include_content=include_content,
        )

        # Merge and deduplicate results
        seen_ids = set()
        merged_results = []

        # Combine results sorted by score
        all_results = sorted(
            faiss_results + pg_results,
            key=lambda x: x.get("score", 0),
            reverse=True,
        )

        for result in all_results:
            doc_id = result.get("metadata", {}).get("document_id")
            offset_start = result.get("metadata", {}).get("offset_start")

            # Create unique identifier for deduplication
            result_id = (doc_id, offset_start)

            if result_id not in seen_ids:
                seen_ids.add(result_id)
                result["source"] = "hybrid"
                merged_results.append(result)

                if len(merged_results) >= k:
                    break

        return merged_results

    async def _enrich_faiss_results(
        self,
        faiss_results: List[Dict[str, Any]],
        include_content: bool,
        document_id_filter: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Enrich FAISS results with content from the database.

        FAISS only stores embeddings and metadata, so we need to fetch
        the actual content from PostgreSQL if needed.
        """
        if not faiss_results:
            return []

        enriched = []

        for result in faiss_results:
            meta = result.get("metadata", {}).copy()

            # Apply document_id filter
            if document_id_filter is not None and meta.get("document_id") != document_id_filter:
                continue

            doc_id = meta.get("document_id")

            # Fetch document and fund names
            if doc_id:
                doc_title, fund_name = await self._fetch_document_and_fund_names(doc_id)
                if doc_title:
                    meta["document_title"] = doc_title
                if fund_name:
                    meta["fund_name"] = fund_name

            enriched_result = {
                "metadata": meta,
                "score": result.get("score", 0.0),
            }

            # Fetch content if needed
            if include_content:
                offset_start = meta.get("offset_start")

                if doc_id and offset_start is not None:
                    content = await self._fetch_content_from_db(doc_id, offset_start)
                    if content:
                        enriched_result["content"] = content

            enriched.append(enriched_result)

        return enriched

    async def _fetch_content_from_db(
        self,
        document_id: int,
        offset_start: int,
    ) -> Optional[str]:
        """Fetch content from the database for a specific chunk."""
        try:
            from sqlalchemy import text

            query = text("""
                SELECT content
                FROM document_embeddings
                WHERE document_id = :document_id
                  AND metadata->>'offset_start' = :offset_start
                LIMIT 1
            """)

            result = self.db.execute(
                query,
                {"document_id": document_id, "offset_start": str(offset_start)},
            ).fetchone()

            if result:
                return result[0]
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "Failed to fetch content for document %s, offset %s: %s",
                document_id,
                offset_start,
                exc,
            )

        return None

    async def _fetch_document_and_fund_names(
        self,
        document_id: int,
    ) -> tuple[Optional[str], Optional[str]]:
        """Fetch document title and fund name from the database."""
        try:
            from sqlalchemy import text

            query = text("""
                SELECT d.file_name, f.name
                FROM documents d
                LEFT JOIN funds f ON d.fund_id = f.id
                WHERE d.id = :document_id
                LIMIT 1
            """)

            result = self.db.execute(
                query,
                {"document_id": document_id},
            ).fetchone()

            if result:
                return result[0], result[1]
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "Failed to fetch document and fund names for document %s: %s",
                document_id,
                exc,
            )

        return None, None

    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the search service.

        Returns:
            Dictionary with statistics about available backends and index sizes
        """
        stats = {
            "available_backends": [SearchBackend.POSTGRESQL.value],
            "preferred_backend": self.prefer_backend.value,
            "faiss_available": self.faiss_available,
        }

        if self.faiss_available and self.faiss_manager:
            try:
                if self.faiss_manager.index_path.exists():
                    index = self.faiss_manager._load_index()
                    stats["faiss_vectors"] = index.ntotal
                    stats["available_backends"].append(SearchBackend.FAISS.value)
                    stats["available_backends"].append(SearchBackend.HYBRID.value)
            except Exception as exc:  # pragma: no cover - optional
                logger.warning("Failed to get FAISS stats: %s", exc)

        return stats
