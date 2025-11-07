**Movie Data Pipeline — Data Engineering Assignment**

Author: Vaigainathan
Role Applied: Data Engineer (Fresher)
Company: tsworks
Duration: November 2025




 **Project Overview**
This project implements a simple ETL (Extract, Transform, Load) data pipeline for analyzing movie data.
The goal is to combine local CSV datasets (from MovieLens) with additional movie information fetched from the OMDb API, clean and transform the data, and load it into a relational database for further analysis.

This assignment simulates a real-world data engineering workflow — from ingestion to transformation, storage, and analysis.




 **Objectives**
Ingest and clean movie and rating data from multiple sources.

Enrich data using the OMDb API.

Design a normalized relational database schema.

Load data into the database efficiently and idempotently.

Write analytical SQL queries to answer business questions.



** Tech Stack**


Category	Tools / Libraries
Language	Python 3.x
Data Processing	pandas
Database	SQLite (via SQLAlchemy)
API Access	requests
Data Source	MovieLens CSV + OMDb API
Version Control	Git & GitHub
** Project Structure**
movie_data_pipeline/
├── data/
│   ├── movies.csv
│   ├── ratings.csv
├── etl.py              # Main ETL pipeline script
├── schema.sql          # Database schema (CREATE TABLE statements)
├── queries.sql         # Analytical SQL queries
├── requirements.txt    # Python dependencies
└── README.md           # Project documentation




**How to Run the Project**
**Clone the repository**
git clone https://github.com/<your-username>/movie_data_pipeline.git
cd movie_data_pipeline



**Set up the environment**

(Optional but recommended)

python -m venv .venv
.venv\Scripts\activate  # on Windows
pip install -r requirements.txt




**Download MovieLens dataset**

Visit https://grouplens.org/datasets/movielens/latest/

Extract movies.csv and ratings.csv into the data/ folder.



 **Add your OMDb API key**

In etl.py, replace:

OMDB_API_KEY = "your_api_key_here"


with your own key from http://www.omdbapi.com/
.



** Run the ETL pipeline**
python etl.py


This will:

Read CSV data

Fetch additional details from OMDb API

Clean and merge the data

Create tables in movies.db

Load final transformed data




**Database Schema**

Below is a simplified overview of the relational structure:

movies

Column	Type	Description
movie_id	INTEGER	Unique ID
title	TEXT	Movie title
genres	TEXT	Pipe-separated list of genres
director	TEXT	Director name (from OMDb)
year	INTEGER	Release year
box_office	REAL	Box office earnings

ratings

Column	Type	Description
user_id	INTEGER	User who rated
movie_id	INTEGER	Foreign key to movies
rating	REAL	Rating (0.5–5.0)
timestamp	INTEGER	Time of rating
**Analytical Queries**
All queries are stored in queries.sql.




**Example questions answered:**

 Which movie has the highest average rating?

 Top 5 genres with highest average ratings.

 Director with the most movies in the dataset.

Average rating by release year.

Design Choices

SQLite was used for simplicity and portability.

The ETL script is idempotent — running it multiple times won’t create duplicates.

API requests are handled carefully with error checks and retry logic.

Data is cleaned and type-corrected before loading (e.g., year and rating types).





 **Challenges & Solutions**
Challenge	Solution
API limits and missing data	Added delay and fallback logic for unavailable results
Inconsistent movie titles between CSV and OMDb	Implemented fuzzy matching / title normalization
Handling genres	Split `
Idempotent loads	Used primary keys and upsert logic via SQLAlchemy





**Future Improvements**

Automate pipeline scheduling using Airflow or Prefect.

Use PostgreSQL or AWS RDS for production scaling.

Implement Docker for containerized deployment.

Add logging and monitoring for data quality metrics.





**How to Contact**

Vaigainathan
vaigainathan6104@gmail.com




