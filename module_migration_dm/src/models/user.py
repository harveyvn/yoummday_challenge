from sqlalchemy import Column, String, DateTime, func, TIMESTAMP
from .base import Base


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, nullable=False)
    user_name = Column(String(255), unique=True, nullable=False)

    date_created = Column(DateTime(timezone=False), server_default=func.now())
    last_updated = Column(TIMESTAMP, server_default=func.now(), onupdate=func.current_timestamp())

    def __repr__(self):
        return f"<User(id={self.id}, user_name='{self.user_name}')>"
