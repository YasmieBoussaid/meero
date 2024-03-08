from models import Base  # Import the Base and models
from config import engine, Session, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
from constants import AIRBNB_API
import requests
from models import Customer
from sqlalchemy import insert
from customer_data_ingestion import get_customer_raw_data, customer_data_cleaning
from minio import Minio

#Init database, create all tables declared in the models.py file
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)


# Customer data pipeline
def customer_data_pipeline():
    # Init client
    client = Minio("minio-container:9000", MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, secure=False)
    # get raw data
    print("Extracting data from MinIO starting")
    raw_customer_data = get_customer_raw_data(client)
    # Clean data
    print("Cleaning customer data starting")
    cleaned_customer_data = customer_data_cleaning(raw_customer_data)

    # store data in customer table
    print("Storing customer data in SQL DB starting")
    session = Session()
    try:
        for customer_row in cleaned_customer_data:
        
            session.add(Customer(**customer_row))
            session.commit()
        print("Succesfully added data in database")

    except Exception as e:
            session.rollback()
            print(f"Error: {e}")

    finally:
            session.close()


customer_data_pipeline()

#Extract Airbnb data from API
def get_airbnb_listings(**kwargs):
    url = AIRBNB_API
    if kwargs:
        first = True
        for key, value in kwargs.items():
            if first:
                url = f"{url}?{key}={value}"
                first = False
            else:
                url = f"{url}&{key}={value}"

    airbnb_listings = requests.get(url=url).json()

    print(airbnb_listings)

