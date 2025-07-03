from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from module_migration_dm.src.config import SQLALCHEMY_DATABASE_URL
from module_migration_dm.src.utils.logging import log


def test_connection():
    try:
        engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=True)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
        log.info(f"✅ Connection successful, test query returned: {result.scalar()}")
    except SQLAlchemyError as e:
        log.error(f"❌ Connection failed: {e}")


if __name__ == '__main__':
    test_connection()
