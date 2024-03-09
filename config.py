from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import os

DB_URI = os.getenv('DB_URI', 'postgresql://yasmine:password@postgres:5432/concierge_cie')
MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER')
MINIO_ROOT_PASSWORD = os.getenv('MINIO_ROOT_PASSWORD')
BUCKET_NAME = "companies"
engine = create_engine(DB_URI)

# Create a session factory
Session = sessionmaker(bind=engine)

