"""
FAISS index manager for semantic search.

This module provides functionality to manage FAISS (Facebook AI Similarity Search) indexes
for efficient similarity search in high-dimensional spaces. It handles indexing document
embeddings, searching for similar documents, and managing persistence of the index files.

FAISS is a library for efficient similarity search and clustering of dense vectors.
It contains algorithms that search in sets of vectors of any size, up to ones of 
trillions of vectors with various indexing algorithms optimized for different use cases.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import SessionLocal

# Import type hints for better IDE support
if TYPE_CHECKING:
    from numpy.typing import NDArray

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
    """
    Manages FAISS index files for fast vector similarity search operations.
    
    This class handles all aspects of FAISS index management including:
    - Loading and saving FAISS index files
    - Adding new embeddings to the index
    - Searching for similar vectors
    - Rebuilding the index from the database
    - Managing metadata associated with indexed vectors
    
    The class uses an inner product (IP) index for similarity search, which is
    effective for normalized vectors where cosine similarity is computed as 
    the inner product of vectors.
    """

    def __init__(self, db: Optional[Session] = None):
        """
        Initialize the FAISS index manager.
        
        Args:
            db: Optional SQLAlchemy database session. If not provided, 
                a new session will be created using SessionLocal.
                
        Raises:
            RuntimeError: If FAISS is not installed in the environment.
        """
        if not FAISS_AVAILABLE:  # pragma: no cover - runtime guard
            raise RuntimeError(
                "FAISS is not installed. Install faiss-cpu to enable FAISS index management."
            )

        self.db = db or SessionLocal()
        # Determine embedding dimension based on available settings
        # Use 1536 for OpenAI embeddings, OLLAMA_EMBED_DIMENSION for Ollama,
        # or 384 for other models
        self.dimension = (
            1536
            if settings.OPENAI_API_KEY
            else settings.OLLAMA_EMBED_DIMENSION if settings.OLLAMA_BASE_URL else 384
        )
        # Ensure the index directory exists
        self.index_dir = Path(settings.VECTOR_STORE_PATH).resolve()
        self.index_dir.mkdir(parents=True, exist_ok=True)
        # Define file paths for index and metadata
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
        """
        Append new embeddings and metadata to the FAISS index.
        
        This method adds new vectors to the existing FAISS index and updates
        the corresponding metadata file. The embeddings are normalized before
        being added to the index to ensure proper cosine similarity computation.
        
        Args:
            embeddings: Iterable of embedding vectors to add to the index.
                       Each embedding should be a numpy array of the same dimension.
            metadata: Iterable of metadata dictionaries corresponding to each 
                     embedding. Each metadata dict contains information about
                     the source document such as document_id, fund_id, etc.
                     
        Returns:
            None: The method modifies the index and metadata files in-place.
            
        Raises:
            ValueError: If embeddings or metadata lists have different lengths,
                       or if embedding dimensions don't match the index dimension.
        """
        if not FAISS_AVAILABLE:  # pragma: no cover - ensures proper usage
            raise RuntimeError(
                "FAISS is not installed. Install faiss-cpu to enable this feature."
            )

        # Convert iterables to lists for processing
        embeddings = list(embeddings)
        metadata = list(metadata)
        
        # Validate that embeddings and metadata have the same length
        if len(embeddings) != len(metadata):
            raise ValueError(
                f"Number of embeddings ({len(embeddings)}) must match "
                f"number of metadata entries ({len(metadata)})"
            )
        
        # Early return if no embeddings to process
        if not embeddings:
            return

        # Validate embedding dimensions
        for i, emb in enumerate(embeddings):
            if emb.shape[0] != self.dimension:
                raise ValueError(
                    f"Embedding {i} has dimension {emb.shape[0]}, "
                    f"expected {self.dimension}"
                )

        # Load the existing index and metadata
        index = self._load_index()
        metadata_list = self._load_metadata()

        # Normalize each embedding vector and stack them into a matrix
        # Normalize to unit vectors for proper cosine similarity computation
        vectors = np.stack([self._normalize(e) for e in embeddings]).astype("float32")

        # Add vectors to the index
        index.add(vectors)

        # Persist the updated index to file with error handling
        try:
            # Write to a temporary file first to avoid corrupting the index
            temp_index_path = self.index_path.with_suffix('.faiss.tmp')
            faiss.write_index(index, str(temp_index_path))

            # If write succeeded, replace the old index atomically
            if temp_index_path.exists():
                temp_index_path.replace(self.index_path)

        except Exception as exc:
            logger.error("Failed to write FAISS index: %s", exc)
            # Clean up temporary file if it exists
            if temp_index_path.exists():
                temp_index_path.unlink()
            raise

        # Extend the metadata list with new metadata entries
        metadata_list.extend(metadata)

        # Write the updated metadata to file
        self._write_metadata(metadata_list)

    def rebuild_from_database(self, fund_id: Optional[int] = None) -> int:
        """
        Rebuild the FAISS index from all stored embeddings in the database.
        
        This method clears the current FAISS index and rebuilds it from scratch
        using embeddings stored in the database. It can optionally filter by
        fund_id to rebuild only embeddings for a specific fund.
        
        Args:
            fund_id: Optional fund ID to filter embeddings. If provided, only
                     embeddings associated with this fund will be used to rebuild
                     the index. If None, all embeddings will be used.
                     
        Returns:
            int: The total number of vectors in the rebuilt index.
            
        Raises:
            RuntimeError: If FAISS is not available in the environment.
        """
        if not FAISS_AVAILABLE:  # pragma: no cover - ensures proper usage
            raise RuntimeError(
                "FAISS is not installed. Install faiss-cpu to enable this feature."
            )

        # Build SQL query to fetch embeddings and metadata
        query = "SELECT embedding, metadata FROM document_embeddings"
        params: Dict[str, Any] = {}
        
        # Add fund filter if specified
        if fund_id is not None:
            query += " WHERE fund_id = :fund_id"
            params["fund_id"] = fund_id

        # Execute query and fetch all results
        rows = self.db.execute(text(query), params).fetchall()
        
        # If no rows found, clear existing files and return
        if not rows:
            self._clear_files()
            logger.info("No embeddings found to rebuild FAISS index.")
            return 0

        embeddings: List[np.ndarray] = []
        metadata: List[Dict[str, Any]] = []
        
        # Process each row from the database
        for embedding_str, metadata_str in rows:
            # Parse embedding string to numpy array
            try:
                embedding_array = np.array(json.loads(embedding_str), dtype="float32")
                
                # Validate embedding dimension
                if embedding_array.shape[0] != self.dimension:
                    logger.warning(
                        "Embedding dimension mismatch: expected %d, got %d. Skipping.",
                        self.dimension, embedding_array.shape[0]
                    )
                    continue
                    
                embeddings.append(embedding_array)
            except json.JSONDecodeError:
                logger.warning("Failed to parse embedding JSON: %s", embedding_str[:100])
                continue
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Failed to process embedding: %s", exc)
                continue

            # Parse metadata - handle case where it's already a dict
            if isinstance(metadata_str, dict):
                metadata.append(metadata_str)
            else:
                # If it's a string, try to parse as JSON
                try:
                    metadata.append(json.loads(metadata_str))
                except json.JSONDecodeError:
                    logger.warning("Failed to parse metadata JSON: %s", metadata_str[:100])
                    # If parsing fails, store as raw string in a dict
                    metadata.append({"raw": metadata_str})
                except Exception as exc:
                    logger.warning("Failed to process metadata: %s", exc)
                    metadata.append({"raw": metadata_str})

        # If no valid embeddings were processed, clear files and return
        if not embeddings:
            self._clear_files()
            logger.warning("No valid embeddings to rebuild FAISS index.")
            return 0

        # Create a new FAISS index with inner product metric
        index = faiss.IndexFlatIP(self.dimension)

        # Stack and normalize embeddings, then add to index
        index.add(np.stack([self._normalize(e) for e in embeddings]))

        # Write the new index to file with error handling
        try:
            # Write to a temporary file first to avoid corrupting the index
            temp_index_path = self.index_path.with_suffix('.faiss.tmp')
            faiss.write_index(index, str(temp_index_path))

            # If write succeeded, replace the old index atomically
            if temp_index_path.exists():
                temp_index_path.replace(self.index_path)

        except Exception as exc:
            logger.error("Failed to write FAISS index during rebuild: %s", exc)
            # Clean up temporary file if it exists
            if temp_index_path.exists():
                temp_index_path.unlink()
            raise

        # Write corresponding metadata to file
        self._write_metadata(metadata)

        logger.info("Rebuilt FAISS index with %s vectors.", index.ntotal)
        return index.ntotal

    def search(
        self,
        query_embedding: np.ndarray,
        k: int = 5,
        fund_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search the FAISS index for similar vectors to the query embedding.
        
        This method performs similarity search using the FAISS library to find
        vectors that are most similar to the provided query embedding. Cosine
        similarity is used by default since the vectors are normalized.
        
        Args:
            query_embedding: The query vector to search for similar vectors.
                           Should be a numpy array with the same dimension
                           as the indexed vectors.
            k: The number of top results to return. Default is 5.
            fund_id: Optional fund ID to filter results. If specified, only
                    results associated with this fund will be returned.
                    
        Returns:
            List[Dict[str, Any]]: A list of result dictionaries, each containing:
                - 'metadata': The associated metadata dictionary for the result
                - 'score': The similarity score (higher is more similar)
                - 'index': The index position of the result in the FAISS index
                
        Raises:
            ValueError: If query_embedding has incorrect dimensions.
        """
        if not FAISS_AVAILABLE:  # pragma: no cover - ensures proper usage
            raise RuntimeError(
                "FAISS is not installed. Install faiss-cpu to enable this feature."
            )

        # Validate input parameters
        if not isinstance(query_embedding, np.ndarray):
            raise ValueError(f"Query embedding must be a numpy array, got {type(query_embedding)}")
        
        if query_embedding.shape[0] != self.dimension:
            raise ValueError(
                f"Query embedding dimension {query_embedding.shape[0]} does not match "
                f"index dimension {self.dimension}"
            )
        
        if k <= 0:
            raise ValueError(f"k must be positive, got {k}")
        
        # Check if index file exists
        if not self.index_path.exists():
            logger.warning("FAISS index does not exist. Returning empty results.")
            return []

        try:
            # Load the index and metadata
            index = self._load_index()
            metadata_list = self._load_metadata()

            # Return empty list if index is empty
            if index.ntotal == 0:
                logger.info("FAISS index is empty.")
                return []

            # Normalize the query vector and reshape for FAISS compatibility
            query_vector = self._normalize(query_embedding).astype("float32").reshape(1, -1)

            # Perform the search - retrieve more results than requested to account for potential filters
            distances, indices = index.search(query_vector, min(k * 2, index.ntotal))

            # Filter results by fund_id if specified and format the results
            results = []
            for idx, distance in zip(indices[0], distances[0]):
                # Skip invalid indices (FAISS returns -1 for not found)
                if idx == -1:  # FAISS returns -1 for not found
                    continue

                # Skip if index is out of bounds for metadata
                if idx >= len(metadata_list):
                    logger.warning("Index %s out of bounds for metadata list", idx)
                    continue

                meta = metadata_list[idx]

                # Apply fund_id filter if specified
                if fund_id is not None and meta.get("fund_id") != fund_id:
                    continue

                # Add the result to the list
                results.append(
                    {
                        "metadata": meta,
                        "score": float(distance),  # Convert to Python float for JSON serialization
                        "index": int(idx),  # Convert to Python int for consistency
                    }
                )

                # Stop when we have enough results
                if len(results) >= k:
                    break

            logger.info("FAISS search returned %s results (requested %s)", len(results), k)
            return results

        except Exception as exc:  # pragma: no cover - defensive
            logger.error("Error during FAISS search: %s", exc)
            return []

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _load_index(self) -> FaissIndex:
        """
        Load the FAISS index from file or create a new one if it doesn't exist.

        Returns:
            FaissIndex: The loaded FAISS index instance.
        """
        if not FAISS_AVAILABLE:  # pragma: no cover - ensures proper usage
            raise RuntimeError(
                "FAISS is not installed. Install faiss-cpu to enable this feature."
            )

        if self.index_path.exists():
            try:
                # Attempt to read the existing index file
                index = faiss.read_index(str(self.index_path))

                # Verify the index has the correct dimension
                if index.d != self.dimension:
                    logger.warning(
                        "FAISS index dimension mismatch: expected %d, got %d. Creating new index.",
                        self.dimension, index.d
                    )
                    # Delete corrupted index files
                    self._clear_files()
                    return faiss.IndexFlatIP(self.dimension)

                return index

            except Exception as exc:
                # Handle corrupted or invalid index files
                logger.warning(
                    "Failed to load FAISS index from %s: %s. Creating new index.",
                    self.index_path, exc
                )
                # Delete corrupted files and start fresh
                self._clear_files()
                return faiss.IndexFlatIP(self.dimension)

        return faiss.IndexFlatIP(self.dimension)

    def _load_metadata(self) -> List[Dict[str, Any]]:
        """
        Load metadata from the metadata file.
        
        Attempts to read and parse the metadata JSON file. If the file doesn't
        exist or is invalid JSON, returns an empty list.
        
        Returns:
            List[Dict[str, Any]]: A list of metadata dictionaries.
        """
        if self.metadata_path.exists():
            try:
                return json.loads(self.metadata_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                logger.warning("Failed to parse FAISS metadata file. Starting fresh.")
        return []

    def _write_metadata(self, metadata: List[Dict[str, Any]]) -> None:
        """
        Write metadata to the metadata file.
        
        Args:
            metadata: List of metadata dictionaries to save to file.
        """
        self.metadata_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _clear_files(self) -> None:
        """
        Remove index and metadata files if they exist.
        
        This method is used to completely reset the FAISS index by removing
        the stored files. It's typically called when rebuilding the index
        from scratch.
        """
        if self.index_path.exists():
            os.remove(self.index_path)
        if self.metadata_path.exists():
            os.remove(self.metadata_path)

    @staticmethod
    def _normalize(vector: np.ndarray) -> np.ndarray:
        """
        Normalize a vector to unit length.
        
        This is essential for cosine similarity computation, as it ensures
        that the inner product of normalized vectors equals their cosine similarity.
        
        Args:
            vector: Input vector to normalize.
            
        Returns:
            np.ndarray: The normalized vector with the same direction but unit length.
        """
        norm = np.linalg.norm(vector)
        if norm == 0.0:
            return vector
        return vector / norm
