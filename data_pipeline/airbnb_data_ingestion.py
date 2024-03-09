from data_pipeline.models import AirbnbListing
from constants import AIRBNB_API
from utils import get_address_by_coordinates

import requests
import logging



def get_airbnb_listings_raw_data(limit:int = 100, offset:int = 0,max_limit:int =2000,**kwargs):
    """
        This function extracts the airbnb listing from opendatasoft API.
        Input : 
            limit : int => number of items returned per call
            offset : int => index of first item returned
            max_limit : int => number of wanted items from the API
            **kwargs => named parameters used as query parameters
        Return:
            airbnb_listings : list => list of airbnb listing objects
    """
    logging.info("Calling Opendatasoft API for Airbnb lstings data.")
    airbnb_listings = []
    url = f"{AIRBNB_API}?limit={limit}&offset={offset}"
    if kwargs:
        for key, value in kwargs.items():
            url = f"{url}&{key}={value}"

    response = requests.get(url=url).json()

    airbnb_listings.extend(response["results"])

    # Make recursive call to get_airbnb_listings_raw_data() function until there's no more data to retrieve from API
    curr_index = offset + limit
    total_count = response["total_count"]
    max_count = min(total_count,max_limit,2000)
    if curr_index < max_count:
        next = get_airbnb_listings_raw_data(limit=limit,offset=curr_index,max_limit=max_limit,**kwargs)
        airbnb_listings.extend(next)

    return airbnb_listings


def airbnb_data_cleaning(airbnb_listing_object):
    """
        This function cleans and transforms a single Airbnb listing raw data.
        It converts coordinates to addresses and rename meaningless field names.
        Input : 
            airbnb_listing_object : list => Airbnb listing raw data
        Return:
            airbnb_listing_object : list =>  Airbnb cleaned data
    """
    airbnb_listing_object['room_price'] = airbnb_listing_object['column_10']
    airbnb_listing_object['country'] = airbnb_listing_object['column_19']
    if 'coordinates' in airbnb_listing_object:
        airbnb_listing_object['coordinates_lon'] = airbnb_listing_object.get('coordinates',{}).get('lon')
        airbnb_listing_object['coordinates_lat'] = airbnb_listing_object.get('coordinates',{}).get('lat')
        address = get_address_by_coordinates(airbnb_listing_object['coordinates_lat'],airbnb_listing_object['coordinates_lon'])       
        airbnb_listing_object['address_line_1'] = address[0] + address[1] if address is not None and len(address) >= 2 else None
        airbnb_listing_object['address_line_2'] = address[2] if address is not None and len(address) >= 3 else None
        del airbnb_listing_object['coordinates']

    del airbnb_listing_object['column_10']
    del airbnb_listing_object['column_19']
    del airbnb_listing_object['column_20']

    return airbnb_listing_object

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