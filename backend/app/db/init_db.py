"""
Database initialization
"""
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
    Base.metadata.create_all(bind=engine)

    dimension = (
        1536
        if settings.OPENAI_API_KEY
        else settings.OLLAMA_EMBED_DIMENSION if settings.OLLAMA_BASE_URL else 384
    )

    with SessionLocal() as session:
        try:
            session.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
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
            session.execute(text("DROP INDEX IF EXISTS document_embeddings_embedding_idx"))
            session.execute(text("DROP TABLE IF EXISTS document_embeddings"))
            session.commit()

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
    print("Database tables created successfully!")


if __name__ == "__main__":
    init_db()
