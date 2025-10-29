import io
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app.api.endpoints import documents
from app.core.config import settings


@pytest.mark.asyncio
async def test_upload_document_rejects_non_pdf():
    file = UploadFile(filename="notes.txt", file=io.BytesIO(b"dummy"))
    db = MagicMock()

    with pytest.raises(HTTPException) as exc:
        await documents.upload_document(file=file, fund_id=1, db=db)

    assert exc.value.status_code == 400
    assert "Only PDF files" in exc.value.detail


@pytest.mark.asyncio
async def test_upload_document_enqueues_celery(monkeypatch, tmp_path):
    pdf_content = b"%PDF-1.4\n%"
    upload = UploadFile(filename="report.pdf", file=io.BytesIO(pdf_content))

    db = MagicMock()
    db.refresh.side_effect = lambda doc: setattr(doc, "id", 99)

    class StubDocument:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    monkeypatch.setattr(documents, "Document", StubDocument)
    monkeypatch.setattr(settings, "UPLOAD_DIR", str(tmp_path))

    # Patch Celery task delay to avoid queue interaction
    fake_result = SimpleNamespace(id="task-123")
    mocked_delay = MagicMock(return_value=fake_result)
    monkeypatch.setattr(documents.process_document_task, "delay", mocked_delay)

    response = await documents.upload_document(file=upload, fund_id=7, db=db)

    assert response.document_id == 99
    assert response.task_id == "task-123"
    mocked_delay.assert_called_once()
    args = mocked_delay.call_args[0]
    assert args[0] == 99
    assert args[1].endswith("report.pdf")
    assert args[2] == 7
    saved_files = list(tmp_path.glob("*report.pdf"))
    assert len(saved_files) == 1


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
