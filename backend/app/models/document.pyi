from __future__ import annotations

from datetime import datetime
from typing import Optional

class Document:
    id: int
    fund_id: Optional[int]
    file_name: str
    file_path: Optional[str]
    upload_date: datetime
    parsing_status: str
    error_message: Optional[str]
