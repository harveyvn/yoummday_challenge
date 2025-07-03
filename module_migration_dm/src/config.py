import os
from dotenv import load_dotenv

if os.path.exists(".encrypted_dm_env") is False:
    load_dotenv(dotenv_path="src/.encrypted_dm_env")
else:
    load_dotenv(dotenv_path=".encrypted_dm_env")

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
