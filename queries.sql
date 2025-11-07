-- 1. Which movie has the highest average rating?
SELECT m.title, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id
ORDER BY avg_rating DESC
LIMIT 1;

-- 2. Top 5 genres with highest average rating
SELECT m.genres, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.genres
ORDER BY avg_rating DESC
LIMIT 5;

-- 3. Director with the most movies
SELECT director, COUNT(*) AS movie_count
FROM movies
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- 4. Average rating of movies released each year
SELECT m.year, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.year
ORDER BY m.year;
