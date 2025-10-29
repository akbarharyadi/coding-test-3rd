"""
Vector store service using pgvector (PostgreSQL extension).

This module provides a VectorStore class that uses PostgreSQL with the pgvector
extension to store and search document embeddings. It supports multiple embedding
providers (OpenAI, Ollama, HuggingFace) and enables semantic search capabilities
for document retrieval.

Key features:
- Support for multiple embedding models (OpenAI, Ollama, HuggingFace)
- Automatic schema management for pgvector
- Similarity search with optional metadata filtering
- Asynchronous operations for embedding generation
- Proper connection management and error handling

The service enables semantic search capabilities, allowing users to find
documents based on their meaning rather than just keyword matching.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

import httpx
import numpy as np
from sqlalchemy import bindparam, text
from sqlalchemy.types import Integer, Text as SQLText
from sqlalchemy.orm import Session

from langchain_openai import OpenAIEmbeddings
from langchain_community.embeddings import HuggingFaceEmbeddings

from app.core.config import settings
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)


class OllamaEmbeddings:
    """
    Lightweight embedding client for Ollama's /api/embeddings endpoint.
    
    This class provides a simple interface to generate embeddings using an Ollama
    server. It makes HTTP requests to the Ollama API to create vector representations
    of text content.
    
    Args:
        base_url: Base URL of the Ollama server (e.g., "http://localhost:11434")
        model: Name of the embedding model to use (e.g., "all-minilm", "nomic-embed-text")
        timeout: Request timeout in seconds (default: 30.0)
        
    Example:
        >>> embeddings = OllamaEmbeddings(
        ...     base_url="http://localhost:11434",
        ...     model="all-minilm"
        ... )
        >>> result = embeddings.embed_query("Hello, world!")
        >>> len(result)  # Embedding dimension
        384
    """

    def __init__(self, base_url: str, model: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout

    def embed_query(self, text: str) -> List[float]:
        """
        Generate embedding for a text query.
        
        Args:
            text: Input text to generate embedding for
            
        Returns:
            List of floating point values representing the embedding vector
            
        Raises:
            RuntimeError: If the API request fails or response is malformed
            
        Example:
            >>> embeddings = OllamaEmbeddings("http://localhost:11434", "all-minilm")
            >>> embedding = embeddings.embed_query("Sample text")
            >>> len(embedding) > 0
            True
        """
        try:
            response = httpx.post(
                f"{self.base_url}/api/embeddings",
                json={"model": self.model, "prompt": text},
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # pragma: no cover - network failures
            raise RuntimeError(f"Ollama embedding request failed: {exc}") from exc

        embedding = payload.get("embedding")
        if embedding is None:
            raise RuntimeError("Ollama embeddings response missing 'embedding'")
        return embedding


class VectorStore:
    """
    pgvector-based vector store for document embeddings.
    
    This class provides a complete interface for storing, retrieving, and searching
    document embeddings in PostgreSQL using the pgvector extension. It supports
    multiple embedding providers and enables semantic search capabilities.
    
    The service automatically handles schema creation, embedding model selection,
    and efficient similarity searches. It's designed to work with financial documents
    but can be used for any text content that requires semantic search capabilities.
    
    Args:
        db: Optional SQLAlchemy session. If not provided, a new session is created.
        
    Example:
        >>> from app.services.vector_store import VectorStore
        >>> vector_store = VectorStore()
        >>> import asyncio
        >>> async def example():
        ...     # Add a document to the store
        ...     metadata = {"document_id": 123, "fund_id": 456}
        ...     await vector_store.add_document("Sample document content", metadata)
        ...     
        ...     # Search for similar content
        ...     results = await vector_store.similarity_search("query text", k=5)
        ...     return len(results) > 0
        >>> # asyncio.run(example())  # Would return True if successful
    """
    
    def __init__(self, db: Optional[Session] = None):
        self.db = db or SessionLocal()
        self.embeddings = self._initialize_embeddings()
        self._ensure_extension()
    
    def _initialize_embeddings(self) -> Any:
        """
        Initialize the appropriate embedding model based on available settings.
        
        This method selects the embedding provider based on configuration settings:
        1. OpenAI if OPENAI_API_KEY is set
        2. Ollama if OLLAMA_BASE_URL is set
        3. Local HuggingFace embeddings as fallback
        
        Returns:
            An embedding client instance appropriate for the selected provider
            
        Example:
            >>> store = VectorStore()
            >>> embeddings = store._initialize_embeddings()
            >>> embeddings is not None
            True
        """
        if settings.OPENAI_API_KEY:
            return OpenAIEmbeddings(
                model=settings.OPENAI_EMBEDDING_MODEL,
                api_key=settings.OPENAI_API_KEY,
            )
        if settings.OLLAMA_BASE_URL:
            return OllamaEmbeddings(
                base_url=settings.OLLAMA_BASE_URL,
                model=settings.OLLAMA_EMBED_MODEL,
            )
        # Fallback to local embeddings
        return HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
    
    def _ensure_extension(self):
        """
        Ensure pgvector extension is enabled and table schema is correct.
        
        This method creates the pgvector extension if it doesn't exist and sets up
        the document_embeddings table with appropriate schema. It also handles
        dimension mismatches by recreating the table if needed.
        
        The schema includes:
        - id: Primary key
        - document_id: Reference to the source document
        - fund_id: Reference to the fund that owns the document
        - content: Original text content
        - embedding: Vector embedding stored in pgvector format
        - metadata: Additional metadata in JSONB format
        - created_at: Timestamp of record creation
        
        Note:
            The embedding dimension varies based on the selected embedding provider:
            - OpenAI: 1536 dimensions
            - Ollama: Configurable via settings (defaults to 384)
            - HuggingFace: 384 dimensions
        """
        try:
            self.db.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            self.db.commit()
            logger.info("pgvector extension ensured successfully")
        except Exception as exc:  # pragma: no cover - dependent on admin rights
            logger.warning("Could not ensure pgvector extension: %s", exc)
            self.db.rollback()

        # Determine embedding dimension based on the selected provider
        dimension = 1536 if settings.OPENAI_API_KEY else (
            settings.OLLAMA_EMBED_DIMENSION if settings.OLLAMA_BASE_URL else 384
        )

        # Check if the table exists and if the dimension matches
        try:
            current_dim = self.db.execute(
                text(
                    """
                    SELECT atttypmod
                    FROM pg_attribute
                    WHERE attrelid = 'document_embeddings'::regclass
                      AND attname = 'embedding'
                    """
                )
            ).scalar()
            
            # Handle dimension mismatch by recreating the table
            if current_dim and int(current_dim) != dimension:
                logger.warning(
                    "document_embeddings embedding dimension mismatch (existing=%s, expected=%s); recreating table",
                    current_dim,
                    dimension,
                )
                self.db.execute(text("DROP INDEX IF EXISTS document_embeddings_embedding_idx"))
                self.db.execute(text("DROP TABLE IF EXISTS document_embeddings"))
                self.db.commit()
        except Exception:
            self.db.rollback()

        # Create the table and index if they don't exist
        try:
            create_table_sql = text(
                f"""
                CREATE TABLE IF NOT EXISTS document_embeddings (
                    id SERIAL PRIMARY KEY,
                    document_id INTEGER,
                    fund_id INTEGER,
                    content TEXT NOT NULL,
                    embedding vector({dimension}),
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            create_index_sql = text(
                """
                CREATE INDEX IF NOT EXISTS document_embeddings_embedding_idx
                ON document_embeddings USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100)
                """
            )

            self.db.execute(create_table_sql)
            self.db.execute(create_index_sql)
            self.db.commit()
            logger.info("Document embeddings table and index created/verified successfully with dimension %d", dimension)
        except Exception as exc:
            logger.error("Error ensuring document_embeddings schema: %s", exc)
            self.db.rollback()
    
    async def add_document(self, content: str, metadata: Dict[str, Any]) -> None:
        """
        Add a document to the vector store with its embedding.
        
        This method generates an embedding for the provided content and stores
        both the content and its vector representation in the database along
        with associated metadata.
        
        Args:
            content: The text content to embed and store
            metadata: Dictionary containing metadata like document_id, fund_id, etc.
            
        Returns:
            None
            
        Raises:
            Exception: If the database operation fails
            
        Example:
            >>> vector_store = VectorStore()
            >>> metadata = {"document_id": 123, "fund_id": 456, "source": "pdf"}
            >>> import asyncio
            >>> # asyncio.run(vector_store.add_document("Sample content", metadata))
        """
        # Input validation
        if not isinstance(content, str) or not content.strip():
            raise ValueError("Content must be a non-empty string")
        
        if not isinstance(metadata, dict):
            raise ValueError("Metadata must be a dictionary")
        
        if "fund_id" not in metadata:
            logger.warning("fund_id not provided in metadata - this may cause retrieval issues")
        
        try:
            # Generate embedding for the content
            embedding = await self._get_embedding(content)
            embedding_list = embedding.tolist()
            
            # Format embedding as a string literal for PostgreSQL - more efficient
            embedding_str = "[" + ",".join(f"{val:.8f}" for val in embedding_list) + "]"

            # SQL query to insert the document and its embedding - optimized using pgvector's casting
            insert_sql = text(
                """
                INSERT INTO document_embeddings (document_id, fund_id, content, embedding, metadata)
                VALUES (:document_id, :fund_id, :content, CAST(:embedding AS vector), CAST(:metadata AS jsonb))
                """
            )

            # Add content length to metadata if not already present
            metadata_with_content = dict(metadata)
            metadata_with_content.setdefault("length", len(content))

            # Execute the insert operation
            self.db.execute(
                insert_sql,
                {
                    "document_id": metadata_with_content.get("document_id"),
                    "fund_id": metadata_with_content.get("fund_id"),
                    "content": content,
                    "embedding": embedding_str,
                    "metadata": json.dumps(metadata_with_content),
                },
            )
            self.db.commit()
            
            logger.info(
                "Successfully added document to vector store: fund_id=%s, document_id=%s", 
                metadata_with_content.get("fund_id"), 
                metadata_with_content.get("document_id")
            )
        except Exception as exc:
            logger.error("Error adding document chunk to vector store: %s", exc)
            self.db.rollback()
            raise
    
    async def similarity_search(
        self, 
        query: str, 
        k: int = 5, 
        filter_metadata: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar documents using cosine similarity in the vector store.
        
        This method generates an embedding for the query text and searches for
        the most similar stored documents based on cosine similarity of their
        embeddings. Optional metadata filters can be applied to narrow results.
        
        Args:
            query: Search query text
            k: Number of top results to return (default: 5)
            filter_metadata: Optional dictionary of metadata filters (e.g., {"fund_id": 123})
            
        Returns:
            List of matching documents with their similarity scores, each containing:
            - id: Database record ID
            - document_id: Reference to source document
            - fund_id: Reference to fund that owns document
            - content: Document text content
            - metadata: Additional metadata as dictionary
            - score: Similarity score (1.0 = perfect match, closer to 0.0 = less similar)
            
        Example:
            >>> vector_store = VectorStore()
            >>> import asyncio
            >>> # results = asyncio.run(
            ... #     vector_store.similarity_search("search query", k=3)
            ... # )
            >>> # len(results)  # Would return 3 or fewer results
        """
        # Input validation
        if not isinstance(query, str) or not query.strip():
            raise ValueError("Query must be a non-empty string")
        
        if not isinstance(k, int) or k <= 0:
            raise ValueError("k must be a positive integer")
        
        if filter_metadata is not None and not isinstance(filter_metadata, dict):
            raise ValueError("filter_metadata must be a dictionary or None")
        
        try:
            # Generate embedding for the query
            query_embedding = await self._get_embedding(query)
            embedding_list = query_embedding.tolist()
            
            # Build WHERE clause for optional metadata filters - more flexible approach
            where_conditions = []
            params = {
                "query_embedding": "[" + ",".join(f"{val:.8f}" for val in embedding_list) + "]",
                "k": k
            }
            
            if filter_metadata:
                for key, value in filter_metadata.items():
                    if key in ["document_id", "fund_id"]:
                        where_conditions.append(f"{key} = :{key}")
                        params[key] = value
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # SQL query optimized for performance
            search_sql = text(
                f"""
                SELECT 
                    id,
                    document_id,
                    fund_id,
                    content,
                    metadata,
                    1 - (embedding <=> CAST(:query_embedding AS vector)) as similarity_score
                FROM document_embeddings
                {where_clause}
                ORDER BY embedding <=> CAST(:query_embedding AS vector)
                LIMIT :k
                """
            )
            
            # Execute the search query
            result = self.db.execute(search_sql, params)
            
            # Process and format results in a more efficient manner
            results = []
            for row in result:
                # Parse metadata if it's stored as JSON string
                metadata = row[4]
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except json.JSONDecodeError:
                        pass  # Keep as string if parsing fails
                        
                results.append(
                    {
                        "id": row[0],
                        "document_id": row[1],
                        "fund_id": row[2],
                        "content": row[3],
                        "metadata": metadata,
                        "score": float(row[5]),
                    }
                )
            
            logger.info("Successfully performed similarity search, found %d results", len(results))
            return results
        except Exception as exc:
            logger.error("Error in similarity search: %s", exc)
            return []
    
    async def _get_embedding(self, text: str) -> np.ndarray:
        """
        Generate embedding for text using the configured embedding provider.
        
        This method uses the appropriate method on the embedding provider
        (either embed_query or encode) to create a vector representation
        of the input text. It runs the embedding operation in an executor
        to avoid blocking the event loop.
        
        Args:
            text: Input text to generate embedding for
            
        Returns:
            NumPy array containing the embedding vector with float32 precision
            
        Raises:
            AttributeError: If the embedding provider doesn't implement
                          the expected interface
            RuntimeError: If the embedding provider raises an error
            
        Example:
            >>> vector_store = VectorStore()
            >>> import asyncio
            >>> # embedding = asyncio.run(vector_store._get_embedding("Hello world"))
            >>> # len(embedding) > 0
        """
        if not hasattr(self.embeddings, "embed_query"):
            raise AttributeError("Embedding client does not implement embed_query")

        embedding = await asyncio.get_running_loop().run_in_executor(
            None, self.embeddings.embed_query, text
        )

        return np.array(embedding, dtype=np.float32)
    
    def clear(self, fund_id: Optional[int] = None):
        """
        Clear document embeddings from the vector store.
        
        This method removes embeddings from the store, either all of them
        or filtered by fund_id if specified. This is useful for cleanup
        operations or when removing data for a specific fund.
        
        Args:
            fund_id: Optional fund ID to filter which embeddings to delete.
                    If None, all embeddings in the store will be deleted.
                    
        Returns:
            None
            
        Example:
            >>> vector_store = VectorStore()
            >>> # Clear all embeddings
            >>> vector_store.clear()
            >>> # Clear embeddings for a specific fund
            >>> vector_store.clear(fund_id=123)
        """
        try:
            if fund_id:
                # Delete only embeddings for the specified fund
                delete_sql = text("DELETE FROM document_embeddings WHERE fund_id = :fund_id")
                self.db.execute(delete_sql, {"fund_id": fund_id})
                logger.info("Cleared vector store for fund_id: %s", fund_id)
            else:
                # Delete all embeddings
                delete_sql = text("DELETE FROM document_embeddings")
                self.db.execute(delete_sql)
                logger.info("Cleared entire vector store")
            
            self.db.commit()
        except Exception as exc:
            logger.error("Error clearing vector store: %s", exc)
            self.db.rollback()
