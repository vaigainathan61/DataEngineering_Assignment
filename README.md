#  Movie Data Pipeline

A simple data pipeline for ingesting, transforming, and analyzing movie data.

## Overview
This project builds an end-to-end ETL pipeline that:
- Extracts movie & rating data from CSV (MovieLens dataset)
- Enriches movie info via the OMDb API
- Loads cleaned data into a SQLite database
- Runs analytical SQL queries for insights

## Tech Stack
- Python (pandas, requests, sqlalchemy)
- SQLite
- OMDb API
- MovieLens dataset

##  Setup Instructions

1. Clone the repo  
   `git clone https://github.com/yourname/movie-data-pipeline.git`

2. Install dependencies  
   `pip install -r requirements.txt`

3. Add your OMDb API key in `etl.py`

4. Run the ETL pipeline  
   `python etl.py`

5. Run queries manually using:
