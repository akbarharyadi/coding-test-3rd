from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.tasks import document_tasks


@patch("app.tasks.document_tasks.SessionLocal")
def test_process_document_task_document_missing(mock_session_local):
    mock_session = MagicMock()
    mock_session.get.return_value = None
    mock_session_local.return_value = mock_session

    result = document_tasks.process_document_task(document_id=1, file_path="/tmp/missing.pdf", fund_id=2)

    assert result["status"] == "failed"
    assert result["error"] == "Document not found"
    mock_session.close.assert_called_once()


@patch("app.tasks.document_tasks.SessionLocal")
@patch("app.tasks.document_tasks.DocumentProcessor")
def test_process_document_task_success(mock_processor_cls, mock_session_local):
    mock_session = MagicMock()
    document_obj = SimpleNamespace(parsing_status="pending", error_message=None)
    mock_session.get.return_value = document_obj
    mock_session_local.return_value = mock_session

    mock_processor = MagicMock()
    mock_processor.process_document.return_value = {"status": "completed", "document_id": 5, "fund_id": 7}
    mock_processor_cls.return_value = mock_processor

    with patch("asyncio.run", return_value={"status": "completed", "document_id": 5, "fund_id": 7}):
        result = document_tasks.process_document_task(document_id=5, file_path="/tmp/file.pdf", fund_id=7)

    assert result["status"] == "completed"
    mock_processor_cls.assert_called_once_with(db_session=mock_session)
    mock_session.commit.assert_called()
    assert document_obj.parsing_status == "completed"
    mock_session.close.assert_called_once()


@patch("app.tasks.document_tasks.SessionLocal")
@patch("app.tasks.document_tasks.DocumentProcessor")
def test_process_document_task_failure_updates_status(mock_processor_cls, mock_session_local):
    mock_session = MagicMock()
    document_obj = SimpleNamespace(parsing_status="pending", error_message=None)
    mock_session.get.return_value = document_obj
    mock_session_local.return_value = mock_session

    mock_processor = MagicMock()
    mock_processor.process_document.return_value = {
        "status": "failed",
        "document_id": 9,
        "fund_id": 4,
        "error": "parse error",
    }
    mock_processor_cls.return_value = mock_processor

    with patch("asyncio.run", return_value={
        "status": "failed",
        "document_id": 9,
        "fund_id": 4,
        "error": "parse error",
    }):
        result = document_tasks.process_document_task(document_id=9, file_path="/tmp/file.pdf", fund_id=4)

    assert result["status"] == "failed"
    assert document_obj.parsing_status == "failed"
    assert document_obj.error_message == "parse error"
    mock_session.close.assert_called_once()
