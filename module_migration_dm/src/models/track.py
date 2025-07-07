from sqlalchemy import Column, String, DateTime, func, TIMESTAMP, Text
from sqlalchemy.dialects.postgresql import UUID
from .base import Base


class Track(Base):
    __tablename__ = "tracks"

    id = Column(UUID(as_uuid=True), primary_key=True, nullable=False)
    track_id = Column(UUID(as_uuid=True), nullable=False)
    track_name = Column(Text(), nullable=False)
    artist_id = Column(UUID(as_uuid=True), nullable=True)
    artist_name = Column(Text(), nullable=True)
    # release_name = Column(Text(), nullable=True)
    #
    # recording_msid = Column(UUID(as_uuid=True), nullable=False)
    # artist_msid = Column(UUID(as_uuid=True), nullable=False)
    # release_msid = Column(UUID(as_uuid=True), nullable=True)

    date_created = Column(DateTime(timezone=False), server_default=func.now())
    last_updated = Column(TIMESTAMP, server_default=func.now(), onupdate=func.current_timestamp())

