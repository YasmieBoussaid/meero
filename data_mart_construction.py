from sqlalchemy.sql import text
from config import engine


def create_analytics_data(view_name:str):
    """
    This function excutes a SQL query that join the customers to right Airbnb accomodation from 
    the airbnb_listings table then count the number of customers per accomodation per city.
    """
    with engine.connect() as con:

        query = f"""DROP MATERIALIZED VIEW IF EXISTS {view_name};
        CREATE MATERIALIZED VIEW {view_name} AS
        WITH customer_by_accomodation AS (
        SELECT
            c.id as customer_id,
		    c.adress as accomodation_address,
		    c.city as accomodation_city,
		    c.created_at as observation_date,
            a.id as accomodation_id,
		    a.name as accomodation_name
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
        )
        SELECT accomodation_id, accomodation_city, count(customer_id) as number_of_cutomers
        FROM customer_by_accomodation
        GROUP BY 1,2"""

        statement = text(query)
        con.execute(statement)
