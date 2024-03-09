from sqlalchemy import select, Table, MetaData
import pandas as pd
from io import BytesIO
from config import engine, BUCKET_NAME

def store_analytics_data(view_name : str, minioClient):
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