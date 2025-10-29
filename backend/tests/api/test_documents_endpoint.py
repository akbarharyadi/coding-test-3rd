import io
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import BackgroundTasks, HTTPException
from starlette.datastructures import UploadFile

from app.api.endpoints import documents


@pytest.mark.asyncio
async def test_upload_document_rejects_non_pdf():
    file = UploadFile(filename="notes.txt", file=io.BytesIO(b"dummy"))
    db = MagicMock()

    with pytest.raises(HTTPException) as exc:
        await documents.upload_document(
            background_tasks=BackgroundTasks(), file=file, fund_id=1, db=db
        )

    assert exc.value.status_code == 400
    assert "Only PDF files" in exc.value.detail


@pytest.mark.asyncio
async def test_get_document_not_found():
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None

    with pytest.raises(HTTPException) as exc:
        await documents.get_document(document_id=5, db=db)

    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_list_documents_validates_limit():
    db = MagicMock()

    with pytest.raises(HTTPException) as exc:
        await documents.list_documents(limit=-1, db=db)

    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_delete_document_removes_file(tmp_path):
    file_path = tmp_path / "stored.pdf"
    file_path.write_text("content")

    doc_obj = SimpleNamespace(id=1, file_path=str(file_path))

    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = doc_obj

    await documents.delete_document(document_id=1, db=db)

    assert not file_path.exists()
    db.delete.assert_called_once_with(doc_obj)
    db.commit.assert_called_once()
