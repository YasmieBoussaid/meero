from models import Base  # Import the Base and models
from config import engine, Session, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
from models import Customer, AirbnbListing
from customer_data_ingestion import get_customer_raw_data, customer_data_cleaning
from airbnb_data_ingestion import get_airbnb_listings_raw_data, airbnb_data_cleaning
from data_mart_construction import create_analytics_data
from minio import Minio
from sqlalchemy import select, Table, MetaData
import pandas as pd
from io import BytesIO

#Init database, create all tables declared in the models.py file
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)


def customer_data_pipeline(minioClient):
    """
    This function launches the customer data ingestion process to retrieve csv files from MinIO and store the data
    in our database after cleaning
    Input:
        None
    Return:
        None
    """
    
    # get raw data from MinIO
    print("Extracting data from MinIO starting")
    raw_customer_data = get_customer_raw_data(minioClient)
    
    # Clean the raw data
    print("Cleaning customer data starting")
    cleaned_customer_data = customer_data_cleaning(raw_customer_data)

    # Store the cleaned data in customer table
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


def airbnb_data_pipeline():
    """
    This function launches the airbnb data ingestion process to retrieve json data from Opendatasoft API.
    Input:
        None
    Return:
        None
    """
    # get raw data from Opendatasoft
    raw_airbnb_data = get_airbnb_listings_raw_data(refine='column_19:France')

    # Clean the raw data
    cleaned_airbnb_data = list(map(airbnb_data_cleaning, raw_airbnb_data))

    # Store data in Airbnb table
    print("Storing Airbnb data in SQL DB starting")
    session = Session()
    try:
        for airbnb_row in cleaned_airbnb_data:
        
            session.add(AirbnbListing(**airbnb_row))
            session.commit()
        print("Succesfully added data in database")

    except Exception as e:
        session.rollback()
        print(f"Error: {e}")

    finally:
        session.close()      


def store_analytics_data(view_name, minioClient):
    """
    """
    metadata = MetaData()
    materialized_view = Table(view_name, metadata, autoload_with=engine)
    query = select(materialized_view)

    with engine.connect() as connection:
        result = connection.execute(query)
        rows = result.fetchall()
        # convert result into dataframe
        df = pd.DataFrame(rows, columns =['accomodation_id', 'city', 'number_of_customers'])
        # convert data to csv and store it in MinIO
        csv_bytes = df.to_csv().encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)

        # store file in MinIO
        minioClient.put_object('companies',
                       'customer_airbnb_analytics/customers_by_accomodation.csv',
                        data=csv_buffer,
                        length=len(csv_bytes),
                        content_type='application/csv')




#main
# Init client
minioClient = Minio("minio-container:9000", MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, secure=False)
customer_data_pipeline(minioClient)
airbnb_data_pipeline()
create_analytics_data("customers_by_accomodation")
store_analytics_data('customers_by_accomodation', minioClient)


