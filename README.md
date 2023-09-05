# ETL Client

This solution can either be run using venv or Docker. See instructions below for both scenarios.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Running the App Using Virtual Environment (venv)](#running-the-app-using-virtual-environment-venv)
- [Running the App Using Docker](#running-the-app-using-docker)

## Prerequisites

Before you can run the app, make sure you have the following dependencies installed:

- Python 3.8 (for venv)

OR 

- Docker

## Running the App Using Virtual Environment (venv)


1. Clone this repository
2. Create & Activate venv
3. Install requirements
4. Run the API data source: `python -m uvicorn api_data_source.main:app --reload`
5. run main.py: `python main.py`

## Running the App Using Docker


1) In the .env file, ensure `API_HOST` is specified correctly to ensure communication between the API data source and the container. If the API source is being run on the host on port 8000, it can be accessed by the container by setting the value to `http://host.docker.internal:8000`
2) `docker build -t etl_client .`
2) `docker run etl_client`


Possible improvements

- Process CSV straight to output file without need for tempfile. Note: this is not possible with the data from the JSON endpoint