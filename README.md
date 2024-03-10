# meero study case 

This Python project contains the code for capturing data from Opendatasoft API and MinIO datalake, apply transformation on the retrieved data, store them in a database then create a materialized view and store the result in a csv file retrievable from MinIO.

### Setup

To set up your environment, you need to install [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/).

After that, all you need to do is to run `docker-compose build` once and then  `docker-compose up ` to start the services.

The docker-compose file will run MinIO, postgres and the data pipeline services.
MinIO interface can be accessed on localhost:9001
To visualise the postgres database data, you can download [pgAdmin software](https://www.pgadmin.org/download/). Inside the software, in the left panel, right click on `Servers > Register > servers...`
In `General` tab, choose a name for your databse connection, for example `data-concierge`.
In `Connection` tab, enter the following informations:
    - Host name/address : localhost
    - Port : 5432
    - Username : yasmine
    - Password : password


### File tree description
- meero : root directory for the code
    - config.py : DB config and extraction of environment variables
    - constants : constants defintion
    - requirements.txt : python modules needed for the code to run
    - utils.py : contains reusable functions such as converting coordinates to addresses and get city name by zip code.
    - data_pipeline : code directory for data pipeline processes
        - models.py : Database models definition
        - airbnb_data_ingestion.py : Airbnb data collection, cleaning and storage in db
        - customer_data_ingestion.py : Customer data collection, cleaning and storage in db
        - data_mart_construction.py : Python and SQL code that creates the materialized view that responds to the use case
        - analytics_data_storage.py : Stores view result in MinIO
    - main.py : Triggers the data pipeline execution from table initialization and data collection to stroing the the result in csv format in MinIO.

