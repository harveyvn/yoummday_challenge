from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_name = Column(String(255), unique=True, nullable=False)

    def __repr__(self):
        return f"<User(id={self.id}, user_name='{self.user_name}')>"
