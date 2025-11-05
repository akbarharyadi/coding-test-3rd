"""
Semantic search Pydantic schemas.
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from enum import Enum


class SearchBackend(str, Enum):
    """Available search backends."""

    POSTGRESQL = "postgresql"
    FAISS = "faiss"
    HYBRID = "hybrid"


class SearchRequest(BaseModel):
    """
    Schema for semantic search request parameters.
    
    Attributes:
        query: The search query text to find relevant documents
        k: Number of top results to return (between 1 and 100)
        fund_id: Optional fund ID to filter results by specific fund
        document_id: Optional document ID to filter results by specific document
        backend: Preferred search backend (will be auto-selected if not specified)
        include_content: Whether to include full document content in the results
    """

    query: str = Field(
        ..., 
        description="The search query text to find relevant documents", 
        min_length=1,
        max_length=1000,
        example="capital call distribution Q4 2023"
    )
    k: int = Field(
        5, 
        description="Number of top results to return", 
        ge=1, 
        le=100,
        example=5
    )
    fund_id: Optional[int] = Field(
        None, 
        description="Filter results by specific fund ID", 
        ge=1,
        example=123
    )
    document_id: Optional[int] = Field(
        None, 
        description="Filter results by specific document ID", 
        ge=1,
        example=456
    )
    backend: Optional[SearchBackend] = Field(
        None, 
        description="Preferred search backend to use (auto-selected if not specified)",
        example="faiss"
    )
    include_content: bool = Field(
        True, 
        description="Whether to include full document content in the search results"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "query": "capital call distribution Q4",
                "k": 5,
                "fund_id": 123,
                "backend": "faiss",
                "include_content": True,
            }
        }


class SearchResultMetadata(BaseModel):
    """
    Schema for metadata associated with a search result.
    
    Attributes:
        document_id: The ID of the document that contains the result
        fund_id: The ID of the fund related to the document
        offset_start: Start offset of the text in the original document
        offset_end: End offset of the text in the original document
        page_number: Page number where the content was found (if applicable)
        length: Length of the content in characters
    """

    document_id: Optional[int] = Field(
        None, 
        description="The ID of the document that contains the search result",
        ge=1
    )
    fund_id: Optional[int] = Field(
        None, 
        description="The ID of the fund related to this document",
        ge=1
    )
    offset_start: Optional[int] = Field(
        None, 
        description="Start offset of the matched text in the original document",
        ge=0
    )
    offset_end: Optional[int] = Field(
        None, 
        description="End offset of the matched text in the original document",
        ge=0
    )
    page_number: Optional[int] = Field(
        None, 
        description="Page number where the content was found in the document",
        ge=1
    )
    length: Optional[int] = Field(
        None, 
        description="Length of the matched content in characters",
        ge=0
    )


class SearchResult(BaseModel):
    """
    Schema for an individual search result.
    
    Attributes:
        content: The text content of the search result
        metadata: Metadata dictionary with additional information about the result
        score: Similarity score between 0 and 1 (higher values indicate higher similarity)
        source: The backend system that provided this result
    """

    content: Optional[str] = Field(
        None, 
        description="The text content of the search result",
        example="Capital call of $1,000,000 was issued on..."
    )
    metadata: Dict[str, Any] = Field(
        ..., 
        description="Metadata dictionary with additional information about the search result"
    )
    score: float = Field(
        ..., 
        description="Similarity score between 0 and 1 (higher values indicate higher similarity)",
        ge=0.0,
        le=1.0,
        example=0.89
    )
    source: str = Field(
        ..., 
        description="The backend system that provided this search result",
        example="faiss"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "content": "Capital call of $1,000,000 was issued on...",
                "metadata": {
                    "document_id": 123,
                    "fund_id": 456,
                    "page_number": 3,
                    "offset_start": 100,
                    "offset_end": 500,
                },
                "score": 0.89,
                "source": "faiss",
            }
        }


class SearchResponse(BaseModel):
    """
    Schema for the semantic search API response.
    
    Attributes:
        results: List of search results matching the query
        total: The total number of results in the response
        query: The original search query that was executed
        backend_used: The search backend that processed the request
        processing_time: Time taken to process the search in seconds (optional)
    """

    results: List[SearchResult] = Field(
        ..., 
        description="List of search results matching the query",
        min_length=0
    )
    total: int = Field(
        ..., 
        description="The total number of results returned in this response",
        ge=0,
        example=5
    )
    query: str = Field(
        ..., 
        description="The original search query that was executed",
        example="capital call distribution Q4 2023"
    )
    backend_used: str = Field(
        ..., 
        description="The search backend that processed the request",
        example="faiss"
    )
    processing_time: Optional[float] = Field(
        None, 
        description="Time taken to process the search request in seconds",
        ge=0.0,
        example=0.15
    )

    class Config:
        json_schema_extra = {
            "example": {
                "results": [
                    {
                        "content": "Capital call of $1,000,000...",
                        "metadata": {"document_id": 123, "fund_id": 456},
                        "score": 0.89,
                        "source": "faiss",
                    }
                ],
                "total": 1,
                "query": "capital call",
                "backend_used": "faiss",
                "processing_time": 0.15,
            }
        }


class SearchStatsResponse(BaseModel):
    """
    Schema for search service statistics response.
    
    Attributes:
        available_backends: List of available search backend options
        preferred_backend: The default search backend that will be used if none specified
        faiss_available: Indicates whether the FAISS backend is available for use
        faiss_vectors: Count of vectors in the FAISS index (None if FAISS is not available)
        postgresql_available: Indicates whether the PostgreSQL backend is available for use
    """

    available_backends: List[str] = Field(
        ..., 
        description="List of available search backend options",
        example=["postgresql", "faiss", "hybrid"]
    )
    preferred_backend: str = Field(
        ..., 
        description="The default search backend that will be used if none specified",
        example="faiss"
    )
    faiss_available: bool = Field(
        ..., 
        description="Indicates whether the FAISS backend is available for use",
        example=True
    )
    faiss_vectors: Optional[int] = Field(
        None, 
        description="Count of vectors in the FAISS index (None if FAISS is not available)",
        ge=0,
        example=1234
    )
    postgresql_available: bool = Field(
        True, 
        description="Indicates whether the PostgreSQL backend is available for use",
        example=True
    )

    class Config:
        json_schema_extra = {
            "example": {
                "available_backends": ["postgresql", "faiss", "hybrid"],
                "preferred_backend": "faiss",
                "faiss_available": True,
                "faiss_vectors": 1234,
                "postgresql_available": True,
            }
        }
