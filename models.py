from uuid import uuid4
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, BIGINT

Base = declarative_base()

class AirbnbListing(Base):
    __tablename__ = 'airbnb_listings'
    uuid = Column(UUID(as_uuid=True), default=uuid4, primary_key=True)
    id = Column(Integer)
    name = Column(String)
    host_id = Column(Integer)
    address_line_1 = Column(String)
    address_line_2 = Column(String)
    neighbourhood = Column(String)
    city = Column(String)
    country = Column(String)
    room_type = Column(String)
    room_price = Column(Integer)
    minimum_nights = Column(Integer)
    number_of_reviews = Column(Integer)
    last_review = Column(Date)
    reviews_per_month = Column(Float)
    calculated_host_listings_count = Column(Integer)
    availability_365 = Column(Integer)
    updated_date = Column(Date)
    coordinates_lon = Column(Float)
    coordinates_lat = Column(Float)
    
    def __repr__(self):
        return "<AirbnbListing(id='{}', name='{}', host_id={})>"\
                .format(self.id, self.name, self.host_id)
    

class Customer(Base):
    __tablename__ = 'customer'
    id = Column(BIGINT, primary_key=True)
    adress = Column(String)
    city = Column(String)
    zip = Column(Integer)
    created_at = Column(Date)
    
    def __repr__(self):
        return "<Customer(id='{}')>"\
                .format(self.id)
    

