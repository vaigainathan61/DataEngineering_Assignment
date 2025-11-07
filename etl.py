# import pandas as pd
# import requests
# import sqlite3
# from sqlalchemy import create_engine
# import time

# # ========== CONFIGURATION ==========
# MOVIES_CSV = "data/movies.csv"
# RATINGS_CSV = "data/ratings.csv"
# DB_NAME = "movies.db"
# OMDB_API_KEY = "4064d611"   # Replace with your actual API key
# OMDB_URL = "http://www.omdbapi.com/"


# # ========== DATABASE SETUP ==========
# engine = create_engine(f"sqlite:///{DB_NAME}")
# conn = sqlite3.connect(DB_NAME)
# cursor = conn.cursor()

# # Run schema.sql file
# with open("schema.sql", "r") as f:
#     cursor.executescript(f.read())
# conn.commit()

# # ========== EXTRACT ==========
# print("Extracting CSV data...")
# movies_df = pd.read_csv(MOVIES_CSV)
# ratings_df = pd.read_csv(RATINGS_CSV)

# # ========== TRANSFORM ==========
# print("Fetching OMDb API data...")

# def fetch_movie_details(title):
#     try:
#         params = {"t": title, "apikey": OMDB_API_KEY}
#         response = requests.get(OMDB_URL, params=params)
#         data = response.json()
#         if data.get("Response") == "True":
#             return {
#                 "Director": data.get("Director"),
#                 "Plot": data.get("Plot"),
#                 "BoxOffice": data.get("BoxOffice"),
#                 "Year": data.get("Year")
#             }
#         else:
#             return {"Director": None, "Plot": None, "BoxOffice": None, "Year": None}
#     except Exception as e:
#         print("Error fetching:", title, e)
#         return {"Director": None, "Plot": None, "BoxOffice": None, "Year": None}

# # Apply API enrichment for each movie
# enriched_data = []
# for idx, row in movies_df.iterrows():
#     details = fetch_movie_details(row['title'])
#     enriched_data.append({
#         "movie_id": row['movieId'],
#         "title": row['title'],
#         "genres": row['genres'],
#         "director": details["Director"],
#         "plot": details["Plot"],
#         "box_office": details["BoxOffice"],
#         "year": details["Year"]
#     })
#     time.sleep(1)  # to avoid hitting API limits

# enriched_df = pd.DataFrame(enriched_data)

# # ========== LOAD ==========
# print("Loading data into SQLite database...")

# # Load movies
# enriched_df.to_sql("movies", engine, if_exists="replace", index=False)

# # Load ratings
# ratings_df.to_sql("ratings", engine, if_exists="replace", index=False)

# print("ETL Process Completed Successfully ✅")

# conn.close()

# import pandas as pd
# import requests
# import sqlite3
# from sqlalchemy import create_engine

# # ========== CONFIGURATION ==========
# MOVIES_CSV = "data/movies.csv"
# RATINGS_CSV = "data/ratings.csv"
# DB_NAME = "movies.db"
# OMDB_API_KEY = "4064d611"   # your real key
# OMDB_URL = "http://www.omdbapi.com/"

# # ========== DATABASE SETUP ==========
# engine = create_engine(f"sqlite:///{DB_NAME}")
# conn = sqlite3.connect(DB_NAME)
# cursor = conn.cursor()

# # Run schema.sql file
# with open("schema.sql", "r") as f:
#     cursor.executescript(f.read())
# conn.commit()

# # ========== EXTRACT ==========
# print("📥 Extracting CSV data...")
# movies_df = pd.read_csv(MOVIES_CSV)
# ratings_df = pd.read_csv(RATINGS_CSV)

# # ========== TRANSFORM ==========
# print("🎬 Fetching OMDb API data (testing small batch)...")

# def fetch_movie_details(title):
#     """Fetch director, plot, box office, and year from OMDb."""
#     try:
#         params = {"t": title, "apikey": OMDB_API_KEY}
#         response = requests.get(OMDB_URL, params=params, timeout=5)
#         data = response.json()
#         if data.get("Response") == "True":
#             return {
#                 "Director": data.get("Director"),
#                 "Plot": data.get("Plot"),
#                 "BoxOffice": data.get("BoxOffice"),
#                 "Year": data.get("Year")
#             }
#         else:
#             print(f"⚠️ Not found: {title}")
#             return {"Director": None, "Plot": None, "BoxOffice": None, "Year": None}
#     except Exception as e:
#         print(f"❌ Error fetching {title}: {e}")
#         return {"Director": None, "Plot": None, "BoxOffice": None, "Year": None}

# # Fetch details for first 15 movies only (for fast testing)
# sample_movies = movies_df.head(15)
# enriched_data = []

# for idx, row in sample_movies.iterrows():
#     print(f"🔎 Fetching: {row['title']}")
#     details = fetch_movie_details(row['title'])
#     enriched_data.append({
#         "movie_id": row['movieId'],
#         "title": row['title'],
#         "genres": row['genres'],
#         "director": details["Director"],
#         "plot": details["Plot"],
#         "box_office": details["BoxOffice"],
#         "year": details["Year"]
#     })

# enriched_df = pd.DataFrame(enriched_data)

# # ========== LOAD ==========
# print("💾 Loading data into SQLite database...")

# enriched_df.to_sql("movies", engine, if_exists="replace", index=False)
# ratings_df.to_sql("ratings", engine, if_exists="replace", index=False)

# print("✅ ETL Process Completed Successfully!")

# conn.close()




# import pandas as pd
# import requests
# import sqlite3
# import re
# from sqlalchemy import create_engine

# # ========== CONFIGURATION ==========
# MOVIES_CSV = "data/movies.csv"
# RATINGS_CSV = "data/ratings.csv"
# DB_NAME = "movies.db"
# OMDB_API_KEY = "4064d611"   # Replace with your real OMDb API key
# OMDB_URL =  "http://www.omdbapi.com/"


# # ========== DATABASE SETUP ==========
# engine = create_engine(f"sqlite:///{DB_NAME}")
# conn = sqlite3.connect(DB_NAME)
# cursor = conn.cursor()

# # Run schema.sql file
# with open("schema.sql", "r") as f:
#     cursor.executescript(f.read())
# conn.commit()

# # ========== EXTRACT ==========
# print("📥 Extracting CSV data...")
# movies_df = pd.read_csv(MOVIES_CSV)
# ratings_df = pd.read_csv(RATINGS_CSV)

# # ========== TRANSFORM ==========
# print("🎬 Fetching OMDb API data (first 15 movies for testing)...")

# def fetch_movie_details(title):
#     """Fetch director, plot, box office, and year from OMDb."""
#     try:
#         # Extract title and year from "Movie (1995)" format
#         match = re.match(r"^(.*)\s\((\d{4})\)$", title)
#         if match:
#             movie_title = match.group(1)
#             year = match.group(2)
#         else:
#             movie_title = title
#             year = None

#         params = {"t": movie_title, "apikey": OMDB_API_KEY}
#         if year:
#             params["y"] = year

#         response = requests.get(OMDB_URL, params=params, timeout=5)
#         data = response.json()

#         if data.get("Response") == "True":
#             print(f"✅ Found: {movie_title} ({year})")
#             return {
#                 "Director": data.get("Director"),
#                 "Plot": data.get("Plot"),
#                 "BoxOffice": data.get("BoxOffice"),
#                 "Year": data.get("Year")
#             }
#         else:
#             print(f"⚠️ Not found: {movie_title} ({year})")
#             return {"Director": None, "Plot": None, "BoxOffice": None, "Year": None}

#     except Exception as e:
#         print(f"❌ Error fetching {title}: {e}")
#         return {"Director": None, "Plot": None, "BoxOffice": None, "Year": None}

# # Fetch a small batch for testing (speed)
# sample_movies = movies_df.head(15)
# enriched_data = []

# for idx, row in sample_movies.iterrows():
#     details = fetch_movie_details(row['title'])
#     enriched_data.append({
#         "movie_id": row['movieId'],
#         "title": row['title'],
#         "genres": row['genres'],
#         "director": details["Director"],
#         "plot": details["Plot"],
#         "box_office": details["BoxOffice"],
#         "year": details["Year"]
#     })

# enriched_df = pd.DataFrame(enriched_data)

# # ========== LOAD ==========
# print("💾 Loading data into SQLite database...")

# enriched_df.to_sql("movies", engine, if_exists="replace", index=False)
# ratings_df.to_sql("ratings", engine, if_exists="replace", index=False)

# print("✅ ETL Process Completed Successfully!")

# conn.close()


import pandas as pd
import requests
import sqlite3
from sqlalchemy import create_engine
import re  # for title-year extraction

# ========== CONFIGURATION ==========
MOVIES_CSV = "data/movies.csv"
RATINGS_CSV = "data/ratings.csv"
DB_NAME = "movies.db"
OMDB_API_KEY = "4064d611"   # Replace with your actual OMDb API key
OMDB_URL = "http://www.omdbapi.com/"  # ✅ Correct base URL

# ========== DATABASE SETUP ==========
engine = create_engine(f"sqlite:///{DB_NAME}")
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

# Run schema.sql file
with open("schema.sql", "r") as f:
    cursor.executescript(f.read())
conn.commit()

# ========== EXTRACT ==========
print("📥 Extracting CSV data...")
movies_df = pd.read_csv(MOVIES_CSV)
ratings_df = pd.read_csv(RATINGS_CSV)

# ========== TRANSFORM ==========
print("🎬 Fetching OMDb API data (first 10 movies for test)...")

def fetch_movie_details(title):
    """Fetch director, plot, box office, and year from OMDb."""
    try:
        # Extract clean title and year from "Toy Story (1995)"
        match = re.match(r"^(.*)\s\((\d{4})\)$", title)
        if match:
            movie_title = match.group(1)
            year = match.group(2)
        else:
            movie_title = title
            year = None

        params = {"t": movie_title, "apikey": OMDB_API_KEY}
        if year:
            params["y"] = year

        response = requests.get(OMDB_URL, params=params, timeout=5)
        data = response.json()

        if data.get("Response") == "True":
            print(f"✅ Found: {movie_title} ({year})")
            return {
                "Director": data.get("Director"),
                "Plot": data.get("Plot"),
                "BoxOffice": data.get("BoxOffice"),
                "Year": data.get("Year")
            }
        else:
            print(f"⚠️ Not found: {movie_title} ({year})")
            return {"Director": None, "Plot": None, "BoxOffice": None, "Year": None}

    except Exception as e:
        print(f"❌ Error fetching {title}: {e}")
        return {"Director": None, "Plot": None, "BoxOffice": None, "Year": None}

# Fetch details for first 10 movies to test faster
sample_movies = movies_df.head(10)
enriched_data = []

for idx, row in sample_movies.iterrows():
    details = fetch_movie_details(row['title'])
    enriched_data.append({
        "movie_id": row['movieId'],
        "title": row['title'],
        "genres": row['genres'],
        "director": details["Director"],
        "plot": details["Plot"],
        "box_office": details["BoxOffice"],
        "year": details["Year"]
    })

enriched_df = pd.DataFrame(enriched_data)

# ========== LOAD ==========
print("💾 Loading data into SQLite database...")

enriched_df.to_sql("movies", engine, if_exists="replace", index=False)
ratings_df.to_sql("ratings", engine, if_exists="replace", index=False)

print("✅ ETL Process Completed Successfully!")

conn.close()
