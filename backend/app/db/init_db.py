"""
Database initialization
"""
import sys
from pathlib import Path

# Add parent directory to path to allow imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import text
from app.db.base import Base
from app.db.session import engine, SessionLocal
from app.core.config import settings
# Import models to ensure they are registered with SQLAlchemy
from app.models.fund import Fund  # noqa: F401
from app.models.transaction import CapitalCall, Distribution, Adjustment  # noqa: F401
from app.models.document import Document  # noqa: F401
from app.models.conversation import Conversation, Message  # noqa: F401


def init_db():
    """Initialize database tables"""
    # First, create the pgvector extension
    with SessionLocal() as session:
        try:
            print("Creating pgvector extension...")
            session.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            session.commit()
            print("✓ pgvector extension created")
        except Exception as e:
            print(f"Error creating pgvector extension: {e}")
            session.rollback()
            raise

    # Create other tables
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("✓ Base tables created")

    dimension = (
        1536
        if settings.OPENAI_API_KEY
        else settings.OLLAMA_EMBED_DIMENSION if settings.OLLAMA_BASE_URL else 384
    )
    print(f"Using embedding dimension: {dimension}")

    with SessionLocal() as session:
        try:
            # Check if document_embeddings table exists and get its dimension
            existing_dim = session.execute(
                text(
                    """
                    SELECT atttypmod
                    FROM pg_attribute
                    WHERE attrelid = 'document_embeddings'::regclass
                      AND attname = 'embedding'
                    """
                )
            ).scalar()
        except Exception:
            session.rollback()
            existing_dim = None

        if existing_dim and int(existing_dim) != dimension:
            print(f"Dimension mismatch detected. Recreating table with dimension {dimension}...")
            session.execute(text("DROP INDEX IF EXISTS document_embeddings_embedding_idx"))
            session.execute(text("DROP TABLE IF EXISTS document_embeddings"))
            session.commit()

        # Create document_embeddings table
        print("Creating document_embeddings table...")
        session.execute(
            text(
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
        )
        session.commit()
        print("✓ document_embeddings table created")

        # Create index
        print("Creating vector index...")
        session.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS document_embeddings_embedding_idx
                ON document_embeddings USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100)
                """
            )
        )
        session.commit()
        print("✓ Vector index created")

    print("\n✅ Database initialized successfully!")


if __name__ == "__main__":
    init_db()
