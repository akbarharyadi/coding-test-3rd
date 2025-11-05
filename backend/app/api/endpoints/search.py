"""
Semantic search API endpoints.

This module provides REST API endpoints for semantic search across document embeddings,
enabling users to find relevant documents based on meaning rather than keyword matching.
"""
import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.search import (
    SearchRequest,
    SearchResponse,
    SearchResult,
    SearchStatsResponse,
    SearchBackend,
)
from app.services.search_service import SearchService

router = APIRouter()


@router.post("/", response_model=SearchResponse, summary="Semantic search")
async def semantic_search(
    request: SearchRequest,
    db: Session = Depends(get_db),
):
    """
    Perform semantic search across document embeddings.

    This endpoint enables semantic search, finding documents by meaning rather than
    exact keyword matches. It supports multiple search backends (PostgreSQL, FAISS, hybrid)
    and can filter by fund_id or document_id.

    **Search Backends:**
    - `postgresql`: Uses pgvector for similarity search (good for complex filtering)
    - `faiss`: Uses FAISS for fast in-memory search (best for large datasets)
    - `hybrid`: Combines both backends for comprehensive results

    **Parameters:**
    - `query`: The search query text (required)
    - `k`: Number of results to return (1-100, default: 5)
    - `fund_id`: Filter results by fund ID (optional)
    - `document_id`: Filter results by document ID (optional)
    - `backend`: Preferred search backend (optional, auto-selected if not specified)
    - `include_content`: Include full document content in results (default: true)

    **Returns:**
    - List of matching documents with similarity scores
    - Metadata about each match (document_id, fund_id, page number, etc.)
    - Processing time and backend used

    **Example Request:**
    ```json
    {
      "query": "capital call Q4 2023",
      "k": 5,
      "fund_id": 123,
      "backend": "faiss"
    }
    ```

    **Example Response:**
    ```json
    {
      "results": [
        {
          "content": "Capital call of $1,000,000 issued...",
          "metadata": {"document_id": 123, "fund_id": 456, "page_number": 3},
          "score": 0.89,
          "source": "faiss"
        }
      ],
      "total": 1,
      "query": "capital call Q4 2023",
      "backend_used": "faiss",
      "processing_time": 0.15
    }
    ```
    """
    start_time = time.time()

    try:
        # Initialize search service
        search_service = SearchService(db=db, prefer_backend=request.backend)

        # Perform search
        results = await search_service.search(
            query=request.query,
            k=request.k,
            fund_id=request.fund_id,
            document_id=request.document_id,
            backend=request.backend,
            include_content=request.include_content,
        )

        # Format results
        search_results = [
            SearchResult(
                content=result.get("content"),
                metadata=result.get("metadata", {}),
                score=result.get("score", 0.0),
                source=result.get("source", "unknown"),
            )
            for result in results
        ]

        processing_time = time.time() - start_time

        return SearchResponse(
            results=search_results,
            total=len(search_results),
            query=request.query,
            backend_used=request.backend.value if request.backend else search_service.prefer_backend.value,
            processing_time=round(processing_time, 3),
        )

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Search failed: {str(exc)}",
        )


@router.get("/", response_model=SearchResponse, summary="Semantic search (GET)")
async def semantic_search_get(
    query: str = Query(..., description="Search query text", min_length=1),
    k: int = Query(5, description="Number of results", ge=1, le=100),
    fund_id: Optional[int] = Query(None, description="Filter by fund ID"),
    document_id: Optional[int] = Query(None, description="Filter by document ID"),
    backend: Optional[SearchBackend] = Query(None, description="Preferred backend"),
    include_content: bool = Query(True, description="Include document content"),
    db: Session = Depends(get_db),
):
    """
    Perform semantic search using GET request (query parameters).

    This is a convenience endpoint that accepts search parameters via query string
    instead of JSON body. Useful for simple searches and testing.

    **Example:**
    ```
    GET /api/search?query=capital+call&k=5&fund_id=123
    ```
    """
    # Create request object and delegate to POST handler
    request = SearchRequest(
        query=query,
        k=k,
        fund_id=fund_id,
        document_id=document_id,
        backend=backend,
        include_content=include_content,
    )

    return await semantic_search(request=request, db=db)


@router.get("/stats", response_model=SearchStatsResponse, summary="Search statistics")
async def get_search_stats(db: Session = Depends(get_db)):
    """
    Get statistics about the search service.

    Returns information about:
    - Available search backends
    - Preferred/default backend
    - FAISS availability and index size
    - PostgreSQL availability

    **Example Response:**
    ```json
    {
      "available_backends": ["postgresql", "faiss", "hybrid"],
      "preferred_backend": "faiss",
      "faiss_available": true,
      "faiss_vectors": 1234,
      "postgresql_available": true
    }
    ```
    """
    try:
        search_service = SearchService(db=db)
        stats = search_service.get_stats()

        return SearchStatsResponse(**stats, postgresql_available=True)

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve search stats: {str(exc)}",
        )


@router.post("/rebuild-index", summary="Rebuild FAISS index")
async def rebuild_faiss_index(
    fund_id: Optional[int] = Query(None, description="Rebuild index for specific fund"),
    db: Session = Depends(get_db),
):
    """
    Rebuild the FAISS index from database embeddings.

    This endpoint rebuilds the FAISS index from all embeddings stored in PostgreSQL.
    Use this after:
    - Initial setup
    - Bulk document uploads
    - Index corruption
    - Changing embedding models

    **Parameters:**
    - `fund_id`: Optional fund ID to rebuild index for specific fund only

    **Returns:**
    - Number of vectors added to the index
    - Success message

    **Example Response:**
    ```json
    {
      "message": "FAISS index rebuilt successfully",
      "vectors_indexed": 1234,
      "fund_id": null
    }
    ```
    """
    try:
        from app.services.faiss_index import FAISS_AVAILABLE, FaissIndexManager

        if not FAISS_AVAILABLE:
            raise HTTPException(
                status_code=503,
                detail="FAISS is not available. Install faiss-cpu to use this feature.",
            )

        manager = FaissIndexManager(db=db)
        vector_count = manager.rebuild_from_database(fund_id=fund_id)

        return {
            "message": "FAISS index rebuilt successfully",
            "vectors_indexed": vector_count,
            "fund_id": fund_id,
        }

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to rebuild FAISS index: {str(exc)}",
        )
