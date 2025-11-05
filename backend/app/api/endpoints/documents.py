"""
Document API endpoints module

This module provides REST API endpoints for document management including
upload, retrieval, status checking, listing, and deletion of documents.
Documents are processed asynchronously via Celery tasks to avoid blocking
the API during heavy document extraction and vectorization work.
"""
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Any, List, Optional, cast
from typing import cast as typing_cast
from celery.app.task import Task
import os
import shutil
from datetime import datetime
from app.db.session import get_db
from app.models.document import Document
from app.models.fund import Fund
from app.schemas.document import (
    Document as DocumentSchema,
    DocumentUploadResponse,
    DocumentStatus
)
from app.core.config import settings
from app.tasks.document_tasks import process_document_task as _process_document_task
from app.services.fund_extractor import extract_fund_info_from_segments, extract_fund_info_from_tables
from app.helpers.document_utils import extract_with_docling, extract_with_pdfplumber, TextSegment, TableCandidate

process_document_task: Task = typing_cast(Task, _process_document_task)

router = APIRouter()


@router.post("/upload", response_model=DocumentUploadResponse)
async def upload_document(
    file: UploadFile = File(...),
    fund_id: Optional[int] = None,  # Changed to optional - will auto-create fund if not provided
    db: Session = Depends(get_db)
):
    """
    Upload a PDF document for processing and vectorization.
    
    This endpoint handles file uploads with validation for file type and size,
    saves the document to the configured upload directory, creates a database
    record with initial status 'pending', and initiates background processing
    to parse the document and store it in the vector database for similarity search.
    
    If no fund_id is provided, the system will attempt to extract fund information
    from the document and create a new fund automatically.
    
    Args:
        background_tasks (BackgroundTasks): FastAPI dependency for managing background tasks
        file (UploadFile): The PDF file to upload, provided as multipart form data
        fund_id (Optional[int]): The fund ID to associate with the document (auto-create if not provided)
        db (Session): Database session dependency provided by FastAPI's Depends()
    
    Raises:
        HTTPException: 400 if file type is not PDF or file size exceeds limit
        HTTPException: 400 if file upload fails for any reason during processing
        HTTPException: 500 if there are internal server errors during file operations
    
    Returns:
        DocumentUploadResponse: Response containing document ID, task ID, status and message
    """
    # Validate file type - only PDF files are accepted for document processing
    if not file.filename:
        raise HTTPException(status_code=400, detail="File name is required")
    
    safe_filename = file.filename.lower()
    if not safe_filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed for processing")
    
    # Clean and validate filename to prevent path traversal vulnerabilities
    filename = os.path.basename(file.filename)
    if not filename or filename in [".", ".."]:
        raise HTTPException(status_code=400, detail="Invalid filename provided")
    
    # Validate file size efficiently by reading the content in chunks
    # This approach avoids loading the entire file into memory for size checking
    file_size = 0
    
    # Read file in chunks to prevent memory overflow for large files
    try:
        while True:
            chunk = await file.read(8192)  # Read 8KB chunks
            if not chunk:
                break
            file_size += len(chunk)
            
            # Check file size during reading to fail fast if too large
            if file_size > settings.MAX_UPLOAD_SIZE:
                raise HTTPException(
                    status_code=400,
                    detail=f"File size exceeds maximum allowed size of {settings.MAX_UPLOAD_SIZE} bytes"
                )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading uploaded file: {str(e)}")
    
    # Reset file pointer to beginning after size validation
    try:
        await file.seek(0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error resetting file pointer: {str(e)}")
    
    # Ensure upload directory exists before attempting to save file
    # Uses os.makedirs with exist_ok=True to avoid errors if directory already exists
    try:
        os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Error creating upload directory: {str(e)}")
    
    # Generate unique filename using timestamp to prevent conflicts and maintain traceability
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_filename = f"{timestamp}_{filename}"
    file_path = os.path.join(settings.UPLOAD_DIR, unique_filename)
    
    # Save uploaded file to the designated upload directory
    # Using async file operations for better performance and proper error handling
    try:
        with open(file_path, "wb") as buffer:
            # Read and write file in chunks to minimize memory usage
            chunk_size = 8192
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                buffer.write(chunk)
    except OSError as e:
        # Clean up the partially created file if saving fails
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass  # Ignore cleanup errors
        raise HTTPException(status_code=500, detail=f"Error saving file: {str(e)}")
    
    # If no fund_id provided, extract fund information from the PDF and create a new fund
    if fund_id is None:
        try:
            # Extract fund info from the document to create a fund
            fund_info = await _extract_fund_info_from_document(file_path)
            
            # Check if a fund with the same name already exists
            existing_fund = None
            if fund_info.get('fund_name'):
                existing_fund = db.query(Fund).filter(
                    Fund.name.ilike(f"%{fund_info['fund_name']}%")
                ).first()
            
            if existing_fund:
                fund_id = existing_fund.id
            else:
                # Create a new fund with extracted information
                new_fund = Fund(
                    name=fund_info.get('fund_name', f'Fund from {filename}'),
                    gp_name=fund_info.get('gp_name'),
                    vintage_year=fund_info.get('vintage_year'),
                    fund_type="Private Equity"  # Default type, could be extracted if available
                )
                db.add(new_fund)
                db.commit()
                db.refresh(new_fund)
                fund_id = new_fund.id
        except Exception as e:
            # If fund extraction fails, create a generic fund
            new_fund = Fund(
                name=f"Fund from {filename}",
                gp_name=None,
                vintage_year=None
            )
            db.add(new_fund)
            db.commit()
            db.refresh(new_fund)
            fund_id = new_fund.id
    
    # Create database record for the document with initial 'pending' status
    # This allows tracking the processing status even before background processing completes
    try:
        document = Document(
            fund_id=fund_id,
            file_name=filename,
            file_path=file_path,
            parsing_status="pending"
        )
        db.add(document)
        db.commit()
        db.refresh(document)
    except Exception as e:
        # Clean up the saved file if database operation fails
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass  # Ignore cleanup errors
        raise HTTPException(status_code=500, detail=f"Error creating document record: {str(e)}")
    
    # Enqueue Celery task to parse and vectorize the document
    document_id_val = cast(int, getattr(document, "id", 0))
    task_result = process_document_task.delay(
        document_id_val,
        file_path,
        fund_id,
    )

    return DocumentUploadResponse(
        document_id=document_id_val,
        task_id=task_result.id,
        status="pending",
        message="Document uploaded successfully. Processing task enqueued.",
    )


async def _extract_fund_info_from_document(file_path: str) -> dict:
    """
    Extract fund information from a PDF document.
    
    Args:
        file_path: Path to the PDF file
        
    Returns:
        Dictionary containing extracted fund information
    """
    # Try to extract using docling first (better for structured documents)
    try:
        from docling.document_converter import DocumentConverter
        converter = DocumentConverter()
        tables, text_segments = extract_with_docling(
            file_path=file_path,
            document_id=0,  # We don't have a document ID yet
            fund_id=0,      # We don't have a fund ID yet
            converter=converter,
        )
        
        # Extract fund info from both text and tables
        fund_info = extract_fund_info_from_segments(text_segments)
        fund_info.update(extract_fund_info_from_tables(tables))
        
        return fund_info
    
    except ImportError:
        # If docling isn't available, use pdfplumber
        tables, text_segments = extract_with_pdfplumber(
            file_path=file_path,
            document_id=0,  # We don't have a document ID yet
            fund_id=0,      # We don't have a fund ID yet
        )
        
        # Extract fund info from both text and tables
        fund_info = extract_fund_info_from_segments(text_segments)
        fund_info.update(extract_fund_info_from_tables(tables))
        
        return fund_info


@router.get("/{document_id}/status", response_model=DocumentStatus)
async def get_document_status(document_id: int, db: Session = Depends(get_db)):
    """
    Get document parsing status.
    
    Retrieves the current status of document processing, including whether it's
    pending, processing, completed, or failed. Also returns any error messages
    if the document processing failed.
    
    Args:
        document_id (int): The unique identifier of the document to retrieve status for
        db (Session): Database session dependency provided by FastAPI's Depends()
    
    Raises:
        HTTPException: 404 if the document with the given ID is not found
    
    Returns:
        DocumentStatus: Response containing document ID, status and error message if any
    """
    try:
        document = db.query(Document).filter(Document.id == document_id).first()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error occurred: {str(e)}")
    
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    
    document_obj = cast(Any, document)
    status_payload = DocumentStatus(
        document_id=cast(int, getattr(document_obj, "id", 0)),
        status=cast(str, getattr(document_obj, "parsing_status", "")),
        error_message=cast(Optional[str], getattr(document_obj, "error_message", None))
    )
    
    return status_payload


@router.get("/{document_id}", response_model=DocumentSchema)
async def get_document(document_id: int, db: Session = Depends(get_db)):
    """
    Get document details.
    
    Retrieves complete information about a specific document including its
    metadata, file path, processing status, and creation timestamp.
    
    Args:
        document_id (int): The unique identifier of the document to retrieve
        db (Session): Database session dependency provided by FastAPI's Depends()
    
    Raises:
        HTTPException: 404 if the document with the given ID is not found
    
    Returns:
        DocumentSchema: Response containing complete document information
    """
    try:
        document = db.query(Document).filter(Document.id == document_id).first()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error occurred: {str(e)}")
    
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    
    return document


@router.get("/", response_model=List[DocumentSchema])
async def list_documents(
    fund_id: Optional[int] = None,  # Changed to optional
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    List all documents with optional filtering and pagination.
    
    Retrieves a paginated list of documents, with optional filtering by fund ID.
    The results include document metadata, status, and file information.
    
    Args:
        fund_id (Optional[int]): Filter documents by fund ID if provided
        skip (int): Number of records to skip for pagination (default: 0)
        limit (int): Maximum number of records to return (default: 100, max: 1000)
        db (Session): Database session dependency provided by FastAPI's Depends()
    
    Raises:
        HTTPException: 400 if invalid parameters are provided (e.g., negative skip/limit)
        HTTPException: 500 if database error occurs during query
    
    Returns:
        List[DocumentSchema]: List of document records matching the criteria
    """
    # Validate pagination parameters
    if skip < 0:
        raise HTTPException(status_code=400, detail="Skip parameter must be non-negative")
    if limit < 0:
        raise HTTPException(status_code=400, detail="Limit parameter must be non-negative")
    if limit > 1000:  # Set a reasonable maximum limit to prevent resource exhaustion
        raise HTTPException(status_code=400, detail="Limit parameter cannot exceed 1000")
    
    try:
        query = db.query(Document)
        
        if fund_id is not None:
            query = query.filter(Document.fund_id == fund_id)
        
        documents = query.offset(skip).limit(limit).all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error occurred: {str(e)}")
    
    return documents


@router.delete("/{document_id}")
async def delete_document(document_id: int, db: Session = Depends(get_db)):
    """
    Delete a document and its associated file.
    
    Removes the document record from the database and deletes the physical
    file from the file system. This operation is irreversible.
    
    Args:
        document_id (int): The unique identifier of the document to delete
        db (Session): Database session dependency provided by FastAPI's Depends()
    
    Raises:
        HTTPException: 404 if the document with the given ID is not found
        HTTPException: 500 if there are errors during file deletion or database operations
    
    Returns:
        dict: Success message confirming the deletion
    """
    try:
        document = db.query(Document).filter(Document.id == document_id).first()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error occurred: {str(e)}")
    
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    
    # Delete associated file from the filesystem if it exists
    document_obj = cast(Any, document)
    
    file_path = cast(Optional[str], getattr(document_obj, "file_path", None))
    if file_path:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except OSError as e:
            # Log the error but don't fail the operation if file deletion fails
            # The database record will still be removed
            print(f"Warning: Could not delete file {file_path}: {str(e)}")
    
    # Delete database record
    try:
        db.delete(document_obj)
        db.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error occurred: {str(e)}")
    
    return {"message": "Document deleted successfully"}
