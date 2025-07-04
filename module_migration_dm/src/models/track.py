from sqlalchemy import Column, String, DateTime, func, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from .base import Base


class Track(Base):
    __tablename__ = "tracks"

    id = Column(UUID(as_uuid=True), primary_key=True, nullable=False)
    track_name = Column(String(255), nullable=False)
    artist_name = Column(String(255), nullable=False)
    release_name = Column(String(255), nullable=False)

    recording_msid = Column(UUID(as_uuid=True), nullable=False)
    artist_msid = Column(UUID(as_uuid=True), nullable=False)
    release_msid = Column(UUID(as_uuid=True), nullable=False)

    date_created = Column(DateTime(timezone=False), server_default=func.now())
    last_updated = Column(TIMESTAMP, server_default=func.now(), onupdate=func.current_timestamp())

    def __repr__(self):
        return (
            f"<Track(id={self.track_id}, "
            f"track_name='{self.track_name}', "
            f"artist_name='{self.artist_name}', "
            f"recording_msid='{self.recording_msid}')>"
        )
