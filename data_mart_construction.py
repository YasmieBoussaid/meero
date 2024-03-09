from sqlalchemy.sql import text
from config import engine
import time


def create_analytics_data(view_name:str):
    """
    This function excutes a SQL query that join the customers to right Airbnb accomodation from 
    the airbnb_listings table then count the number of customers relative to the number of listings
    by city in France.
    Input:
        view_name : str => materalized view name
    Return:
        None
    """

    query = f"""DROP MATERIALIZED VIEW IF EXISTS {view_name};
    CREATE MATERIALIZED VIEW {view_name} AS
    WITH customer_by_accomodation AS (
        SELECT
        c.id as customer_id,
		c.adress as accomodation_address,
		c.created_at as observation_date,
        a.id as accomodation_id,
		a.name as accomodation_name,
		a.city as accomodation_city
        FROM
        public.customer AS c
        INNER JOIN public.airbnb_listings AS a
        ON 
                -- Matching entire addresses
                UPPER(a.address_line_1) LIKE UPPER(c.adress)
        AND (
                -- Matching cities
                UPPER(c.city) LIKE UPPER(a.city) || '%'
                OR UPPER(a.city) LIKE UPPER(c.city) || '%'
        )
    ),
    accomodations_by_city AS (
	        SELECT city, count(distinct id) as total_accomodations
	        FROM public.airbnb_listings
	        GROUP BY 1
    ),
    customers_by_city AS (
	        SELECT accomodation_city, count(customer_id) as number_of_cutomers
            FROM customer_by_accomodation   
            GROUP BY 1
    )
    SELECT c.accomodation_city, c.number_of_cutomers, a.total_accomodations
    FROM customers_by_city c
    LEFT JOIN accomodations_by_city a
    ON c.accomodation_city = a.city"""

    statement = text(query)
    with engine.connect() as connection:
            connection.execute(statement)
            connection.commit()
    time.sleep(60)

