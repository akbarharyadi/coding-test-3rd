"""
Fund database model
"""
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db.base import Base


class Fund(Base):
    """Fund model"""
    
    __tablename__ = "funds"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    gp_name = Column(String(255))
    fund_type = Column(String(100))
    vintage_year = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    capital_calls = relationship("CapitalCall", back_populates="fund")
    distributions = relationship("Distribution", back_populates="fund")
    adjustments = relationship("Adjustment", back_populates="fund")
    documents = relationship("Document", back_populates="fund")
    conversations = relationship("Conversation", back_populates="fund")
