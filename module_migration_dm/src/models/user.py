from sqlalchemy import Column, String, DateTime, func, TIMESTAMP, Integer, CHAR, Date
from sqlalchemy.dialects.postgresql import UUID
from .base import Base


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, nullable=False)
    user_name = Column(String(255), unique=True, nullable=False)

    gender = Column(CHAR(1), nullable=True)  # 'm' or 'f'
    age = Column(Integer, nullable=True)
    country = Column(String(100), nullable=True)
    registered = Column(Date, nullable=True)

    date_created = Column(DateTime(timezone=False), server_default=func.now())
    last_updated = Column(TIMESTAMP, server_default=func.now(), onupdate=func.current_timestamp())

    def __repr__(self):
        return f"<User(id={self.id}, user_name='{self.user_name}')>"
