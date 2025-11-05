"""
Chat API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any, List
import uuid
from datetime import datetime
from app.db.session import get_db
from app.models.conversation import Conversation as ConversationModel, Message as MessageModel
from app.models.fund import Fund
from app.schemas.chat import (
    ChatQueryRequest,
    ChatQueryResponse,
    ConversationCreate,
    Conversation,
    ChatMessage
)
from app.services.query_engine import QueryEngine
import json

router = APIRouter()


@router.post("/query", response_model=ChatQueryResponse)
async def process_chat_query(
    request: ChatQueryRequest,
    db: Session = Depends(get_db)
):
    """Process a chat query using RAG"""
    
    # Get conversation history if conversation_id provided
    conversation_history = []
    if request.conversation_id:
        # Get recent messages for this conversation (last 10 messages to avoid sending too much)
        messages_db = db.query(MessageModel).filter(
            MessageModel.conversation_id == request.conversation_id
        ).order_by(MessageModel.timestamp.desc()).limit(10).all()
        conversation_history = [
            {"role": msg.role, "content": msg.content, "timestamp": msg.timestamp}
            for msg in reversed(messages_db)  # Reverse to get chronological order
        ]
    
    # Process query
    query_engine = QueryEngine(db)
    
    # If no fund_id is provided, the QueryEngine will now search across all funds
    # and identify the appropriate fund based on the document results
    response = await query_engine.process_query(
        query=request.query,
        fund_id=request.fund_id,
        conversation_history=conversation_history
    )
    
    # Save the conversation to database
    if request.conversation_id:
        # First, check if this is the first message in the conversation
        existing_messages_count = db.query(MessageModel).filter(
            MessageModel.conversation_id == request.conversation_id
        ).count()
        
        # Create user message
        user_msg = MessageModel(
            conversation_id=request.conversation_id,
            role="user",
            content=request.query,
            timestamp=datetime.utcnow()
        )
        db.add(user_msg)
        
        # Create assistant message
        assistant_msg = MessageModel(
            conversation_id=request.conversation_id,
            role="assistant",
            content=response["answer"],
            timestamp=datetime.utcnow(),
            sources=json.dumps(response.get("sources", [])) if response.get("sources") else None,
            metrics=json.dumps(response.get("metrics", {})) if response.get("metrics") else None
        )
        db.add(assistant_msg)
        
        # Update conversation title if this is the first message in the conversation
        conversation_db = db.query(ConversationModel).filter(
            ConversationModel.conversation_id == request.conversation_id
        ).first()
        if conversation_db and existing_messages_count == 0:  # This is the first message
            conversation_db.title = request.query[:100]  # Use first 100 chars as title
            conversation_db.updated_at = datetime.utcnow()
        
        db.commit()
    
    return ChatQueryResponse(**response)


@router.post("/conversations", response_model=Conversation)
async def create_conversation(request: ConversationCreate, db: Session = Depends(get_db)):
    """Create a new conversation"""
    conversation_id = str(uuid.uuid4())
    
    # Create conversation in database
    # Don't associate with fund by default to keep conversations separate
    new_conversation = ConversationModel(
        conversation_id=conversation_id,
        fund_id=request.fund_id,  # Only set if explicitly provided
        title=None  # Title will be set when first message is added
    )
    db.add(new_conversation)
    db.commit()
    db.refresh(new_conversation)
    
    return Conversation(
        conversation_id=conversation_id,
        fund_id=new_conversation.fund_id,
        title=new_conversation.title,
        messages=[],
        created_at=new_conversation.created_at,
        updated_at=new_conversation.updated_at
    )


@router.get("/conversations", response_model=List[Conversation])
async def list_conversations(
    fund_id: int = None,  # Optional: filter by fund ID
    limit: int = 20,      # Optional: limit number of conversations
    offset: int = 0,      # Optional: pagination offset
    db: Session = Depends(get_db)
):
    """List conversations with optional filters"""
    query = db.query(ConversationModel).order_by(ConversationModel.updated_at.desc())
    
    if fund_id is not None:
        query = query.filter(ConversationModel.fund_id == fund_id)
    
    conversations_db = query.offset(offset).limit(limit).all()
    
    # Convert to response format (without messages to keep it lightweight)
    conversations = []
    for conv_db in conversations_db:
        conversations.append(Conversation(
            conversation_id=conv_db.conversation_id,
            fund_id=conv_db.fund_id,
            title=conv_db.title,  # Include the title in the response
            messages=[],  # Don't include messages in list view for performance
            created_at=conv_db.created_at,
            updated_at=conv_db.updated_at
        ))
    
    return conversations


@router.get("/conversations/{conversation_id}", response_model=Conversation)
async def get_conversation(conversation_id: str, db: Session = Depends(get_db)):
    """Get conversation history with all messages"""
    conversation_db = db.query(ConversationModel).filter(
        ConversationModel.conversation_id == conversation_id
    ).first()
    
    if not conversation_db:
        raise HTTPException(status_code=404, detail="Conversation not found")
    
    # Get all messages for this conversation
    messages_db = db.query(MessageModel).filter(
        MessageModel.conversation_id == conversation_id
    ).order_by(MessageModel.timestamp.asc()).all()
    
    # Convert messages to schema
    messages = [
        ChatMessage(role=msg.role, content=msg.content, timestamp=msg.timestamp)
        for msg in messages_db
    ]
    
    return Conversation(
        conversation_id=conversation_db.conversation_id,
        fund_id=conversation_db.fund_id,
        messages=messages,
        created_at=conversation_db.created_at,
        updated_at=conversation_db.updated_at
    )


@router.delete("/conversations/{conversation_id}")
async def delete_conversation(conversation_id: str, db: Session = Depends(get_db)):
    """Delete a conversation and all its messages"""
    conversation_db = db.query(ConversationModel).filter(
        ConversationModel.conversation_id == conversation_id
    ).first()
    
    if not conversation_db:
        raise HTTPException(status_code=404, detail="Conversation not found")
    
    # Delete all messages for this conversation
    db.query(MessageModel).filter(MessageModel.conversation_id == conversation_id).delete()
    
    # Delete the conversation itself
    db.delete(conversation_db)
    db.commit()
    
    return {"message": "Conversation deleted successfully"}
