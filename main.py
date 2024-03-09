from data_pipeline.models import Base
from data_pipeline.customer_data_ingestion import customer_data_pipeline
from data_pipeline.airbnb_data_ingestion import airbnb_data_pipeline
from data_pipeline.data_mart_construction import create_analytics_data
from data_pipeline.analytics_data_storage import store_analytics_data
from config import engine, Session, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
from constants import view_name

from minio import Minio
from sqlalchemy.sql import text

import time
import logging


if __name__ == "__main__":
    #Init database, drop old tables and create all tables declared in the models.py file
    with engine.connect() as connection:
        connection.execute(text(f"""DROP MATERIALIZED VIEW IF EXISTS {view_name};"""))
        connection.commit()
        time.sleep(30)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    # Init MinIO client
    minioClient = Minio("minio-container:9000", MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, secure=False)
    # Launch customer data pipeline
    customer_data_pipeline(minioClient,Session)
    # Launch customer data pipeline
    airbnb_data_pipeline(Session)
    # Create materalized view for analytics 
    create_analytics_data(view_name)
    # store analytics result in MinIO
    store_analytics_data(view_name, minioClient)
    logging.info("Process done.")


