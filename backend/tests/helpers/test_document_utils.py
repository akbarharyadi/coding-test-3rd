from types import SimpleNamespace

import pytest

from app.helpers.document_utils import (
    TableCandidate,
    TextSegment,
    _docling_table_to_matrix,
    _get_docling_page_number,
    chunk_text_segments,
)


def test_docling_table_to_matrix_handles_spans():
    cells = [
        SimpleNamespace(
            text="Header",
            start_row_offset_idx=0,
            start_col_offset_idx=0,
            row_span=1,
            col_span=2,
        ),
        SimpleNamespace(
            text="Value",
            start_row_offset_idx=1,
            start_col_offset_idx=0,
            row_span=1,
            col_span=1,
        ),
        SimpleNamespace(
            text="Value",
            start_row_offset_idx=1,
            start_col_offset_idx=1,
            row_span=1,
            col_span=1,
        ),
    ]
    data = SimpleNamespace(num_rows=2, num_cols=2, table_cells=cells)
    table = SimpleNamespace(data=data)

    matrix = _docling_table_to_matrix(table)

    assert matrix == [["Header", "Header"], ["Value", "Value"]]


def test_get_docling_page_number_from_provenance():
    provenance = [SimpleNamespace(page_no=3)]

    assert _get_docling_page_number(provenance) == 3
    assert _get_docling_page_number([]) is None
    assert _get_docling_page_number(None) is None


def test_chunk_text_segments_creates_overlapping_chunks():
    segments = [
        TextSegment(page_number=1, text="abcdefghi", document_id=1, fund_id=1),
    ]

    chunks = chunk_text_segments(segments, chunk_size=4, chunk_overlap=2)

    contents = [chunk["content"] for chunk in chunks]
    assert contents[0] == "abcd"
    assert "cd" in contents[1]
    assert all(chunk["metadata"]["document_id"] == 1 for chunk in chunks)


def test_chunk_text_segments_raises_for_invalid_size():
    segments = [
        TextSegment(page_number=1, text="sample text", document_id=1, fund_id=1),
    ]

    with pytest.raises(ValueError):
        chunk_text_segments(segments, chunk_size=0, chunk_overlap=1)
