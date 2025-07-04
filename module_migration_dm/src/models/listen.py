from sqlalchemy import Column, String, DateTime, func, TIMESTAMP, Date, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from .base import Base


class Listen(Base):
    __tablename__ = "listens"

    id = Column(UUID(as_uuid=True), primary_key=True, nullable=False)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    track_id = Column(UUID(as_uuid=True), ForeignKey('tracks.id'), nullable=False)
    listened_at = Column(DateTime, nullable=False)
    date = Column(Date, nullable=False)

    user = relationship('User', back_populates='listens')
    track = relationship('Track', back_populates='listens')

    date_created = Column(DateTime(timezone=False), server_default=func.now())
    last_updated = Column(TIMESTAMP, server_default=func.now(), onupdate=func.current_timestamp())
