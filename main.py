from models import Base  # Import the Base and models
from config import engine, Session, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, BUCKET_NAME
from models import Customer, AirbnbListing
from customer_data_ingestion import get_customer_raw_data, customer_data_cleaning
from airbnb_data_ingestion import get_airbnb_listings_raw_data, airbnb_data_cleaning
from data_mart_construction import create_analytics_data
from minio import Minio
from sqlalchemy import select, Table, MetaData
import pandas as pd
from io import BytesIO
import logging
from sqlalchemy.sql import text
import time




def customer_data_pipeline(minioClient,Session):
    """
    This function launches the customer data ingestion process to retrieve csv files from MinIO and store the data
    in our database after cleaning
    Input:
        minioClient : Minio => Minio object
    Return:
        None
    """
    
    # get raw data from MinIO
    logging.info("Extracting data from MinIO starting...")
    raw_customer_data = get_customer_raw_data(minioClient)
    
    # Clean the raw data
    logging.info("Cleaning customer data starting")
    cleaned_customer_data = customer_data_cleaning(raw_customer_data)

    # Store the cleaned data in customer table
    logging.info("Loading customer data in SQL DB starting")
    session = Session()
    try:
        for customer_row in cleaned_customer_data:
        
            session.add(Customer(**customer_row))
            session.commit()
        logging.info("Succesfully added customer data in database.")

    except Exception as e:
            session.rollback()
            logging.info(f"Error: {e}")

    finally:
            session.close()


def airbnb_data_pipeline(Session):
    """
    This function launches the airbnb data ingestion process to retrieve json data from Opendatasoft API
    and store it in the database.
    Input:
        Session : db Session initializer
    Return:
        None
    """
    # get raw data from Opendatasoft
    logging.info("Airbnb data pipeline starting...")
    raw_airbnb_data = get_airbnb_listings_raw_data(refine='column_19:France')

    # Clean the raw data
    logging.info("Airbnb data cleaning starting...")
    cleaned_airbnb_data = list(map(airbnb_data_cleaning, raw_airbnb_data))

    # Store data in Airbnb table
    logging.info("Loading Airbnb data in database starting...")
    db_session = Session()
    try:
        for airbnb_row in cleaned_airbnb_data:
        
            db_session.add(AirbnbListing(**airbnb_row))
            db_session.commit()
        logging.info("Succesfully added Airbnb data in database")

    except Exception as e:
        db_session.rollback()
        logging.info(f"Error: {e}")

    finally:
        db_session.close()      

def store_analytics_data(view_name, minioClient):
    """
    This function stores the materialized view result in MinIO data storage.
    Input:
        view_name : str => materialized view name
        minioClient : Minio => Minio client object
    Return:
        None
    """

    metadata = MetaData()
    materialized_view = Table(view_name, metadata, autoload_with=engine)
    query = select(materialized_view)

    with engine.connect() as connection:
        result = connection.execute(query)
        rows = result.fetchall()
        # convert result into dataframe
        df = pd.DataFrame(rows, columns =['accomodation_city', 'number_of_customers', 'total_accomodations'])
        # convert data to csv and store it in MinIO
        csv_bytes = df.to_csv().encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)

        # store file in MinIO
        minioClient.put_object(BUCKET_NAME,
                       'customers_accomodation_by_city.csv',
                        data=csv_buffer,
                        length=len(csv_bytes),
                        content_type='application/csv')



if __name__ == "__main__":
    #Init database, drop old tables and create all tables declared in the models.py file
    with engine.connect() as connection:
        connection.execute(text("""DROP MATERIALIZED VIEW IF EXISTS customers_by_accomodation;"""))
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
    create_analytics_data("customers_by_accomodation")
    # store analytics result in MinIO
    store_analytics_data("customers_by_accomodation", minioClient)


