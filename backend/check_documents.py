"""
Quick script to check documents and embeddings for specific funds
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.db.session import SessionLocal
from app.models.document import Document
from app.models.fund import Fund
from sqlalchemy import text

def check_fund_documents():
    """Check documents and embeddings for funds"""
    db = SessionLocal()

    try:
        # Check for Velocity Ventures Fund
        print("=" * 60)
        print("Checking Velocity Ventures Fund...")
        print("=" * 60)

        velocity_fund = db.query(Fund).filter(Fund.name == "Velocity Ventures Fund").first()
        if velocity_fund:
            print(f"✓ Fund found: ID={velocity_fund.id}, Name={velocity_fund.name}")

            # Check documents
            docs = db.query(Document).filter(Document.fund_id == velocity_fund.id).all()
            print(f"\nDocuments for this fund: {len(docs)}")
            for doc in docs:
                print(f"  - {doc.filename} (ID: {doc.id}, Status: {doc.status})")

            # Check embeddings
            embedding_count = db.execute(
                text("SELECT COUNT(*) FROM document_embeddings WHERE fund_id = :fund_id"),
                {"fund_id": velocity_fund.id}
            ).scalar()
            print(f"\nEmbeddings in database: {embedding_count}")
        else:
            print("✗ Fund not found!")

        print("\n" + "=" * 60)
        print("Checking Horizon Digital Fund...")
        print("=" * 60)

        horizon_fund = db.query(Fund).filter(Fund.name == "Horizon Digital Fund").first()
        if horizon_fund:
            print(f"✓ Fund found: ID={horizon_fund.id}, Name={horizon_fund.name}")

            # Check documents
            docs = db.query(Document).filter(Document.fund_id == horizon_fund.id).all()
            print(f"\nDocuments for this fund: {len(docs)}")
            for doc in docs:
                print(f"  - {doc.filename} (ID: {doc.id}, Status: {doc.status})")

            # Check embeddings
            embedding_count = db.execute(
                text("SELECT COUNT(*) FROM document_embeddings WHERE fund_id = :fund_id"),
                {"fund_id": horizon_fund.id}
            ).scalar()
            print(f"\nEmbeddings in database: {embedding_count}")
        else:
            print("✗ Fund not found!")

        # Check total embeddings
        print("\n" + "=" * 60)
        total_embeddings = db.execute(text("SELECT COUNT(*) FROM document_embeddings")).scalar()
        print(f"Total embeddings in database: {total_embeddings}")

        # Check FAISS index file
        from app.core.config import settings
        from pathlib import Path
        faiss_path = Path(settings.VECTOR_STORE_PATH) / "documents.faiss"
        metadata_path = Path(settings.VECTOR_STORE_PATH) / "documents_metadata.json"

        print(f"\nFAISS index file exists: {faiss_path.exists()}")
        if faiss_path.exists():
            print(f"  Size: {faiss_path.stat().st_size} bytes")

        print(f"Metadata file exists: {metadata_path.exists()}")
        if metadata_path.exists():
            print(f"  Size: {metadata_path.stat().st_size} bytes")

    finally:
        db.close()

if __name__ == "__main__":
    check_fund_documents()
