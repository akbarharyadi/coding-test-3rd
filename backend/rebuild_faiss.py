"""
Rebuild FAISS index from database embeddings.

This script rebuilds the FAISS index from all embeddings stored in the database.
Use this to fix corrupted FAISS index files or to rebuild after changing dimensions.

Usage:
    python rebuild_faiss.py
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from app.db.session import SessionLocal
from app.services.faiss_index import FAISS_AVAILABLE, FaissIndexManager


def rebuild_faiss_index():
    """Rebuild the FAISS index from database embeddings"""

    if not FAISS_AVAILABLE:
        print("ERROR: FAISS is not installed. Install with: pip install faiss-cpu")
        return False

    print("Rebuilding FAISS index from database...")

    try:
        with SessionLocal() as session:
            manager = FaissIndexManager(db=session)

            print(f"Using embedding dimension: {manager.dimension}")
            print(f"Index path: {manager.index_path}")
            print(f"Metadata path: {manager.metadata_path}")

            # Rebuild the index
            count = manager.rebuild_from_database()

            if count == 0:
                print("\nWARNING: No embeddings found in database. Index is empty.")
            else:
                print(f"\nSUCCESS: Rebuilt FAISS index with {count} vectors!")

            return True

    except Exception as e:
        print(f"\nERROR: Failed to rebuild FAISS index: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = rebuild_faiss_index()
    sys.exit(0 if success else 1)
