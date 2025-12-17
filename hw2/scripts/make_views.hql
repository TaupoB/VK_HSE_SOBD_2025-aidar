USE movielens;

DROP VIEW IF EXISTS top_rated_movies;
CREATE VIEW top_rated_movies AS
SELECT m.title, COUNT(r.rating) as review_count, ROUND(AVG(r.rating), 2) as avg_rating
FROM ratings r
JOIN movies m ON r.movieId = m.movieId
GROUP BY m.title
HAVING COUNT(r.rating) > 100 AND AVG(r.rating) >= 4.0
ORDER BY avg_rating DESC
LIMIT 20;

DROP VIEW IF EXISTS user_activity_yearly;
CREATE VIEW user_activity_yearly AS
SELECT 
    SUBSTR(tstamp, 1, 4) as activity_year, -- Просто берем первые 4 символа (Год)
    COUNT(*) as total_ratings,
    COUNT(DISTINCT userId) as unique_users
FROM ratings
GROUP BY SUBSTR(tstamp, 1, 4)
ORDER BY activity_year DESC;

DROP VIEW IF EXISTS top_movie_tags;
CREATE VIEW top_movie_tags AS
SELECT title, tag, relevance
FROM (
    SELECT m.title, gt.tag, gs.relevance,
           ROW_NUMBER() OVER (PARTITION BY m.movieId ORDER BY gs.relevance DESC) as rn
    FROM movies m
    JOIN genome_scores gs ON m.movieId = gs.movieId
    JOIN genome_tags gt ON gs.tagId = gt.tagId
) t
WHERE t.rn = 1
ORDER BY relevance DESC
LIMIT 20;

DROP VIEW IF EXISTS active_users_union;
CREATE VIEW active_users_union AS
SELECT u_id FROM (
    SELECT userId as u_id FROM ratings
    UNION
    SELECT userId as u_id FROM tags
) combined_users;

DROP VIEW IF EXISTS genre_analysis;
CREATE VIEW genre_analysis AS
SELECT 
    CASE 
        WHEN genres LIKE '%Comedy%' THEN 'Comedy'
        WHEN genres LIKE '%Drama%' THEN 'Drama'
        ELSE 'Other'
    END as main_genre,
    COUNT(*) as rating_count,
    ROUND(AVG(rating), 2) as avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
WHERE genres LIKE '%Comedy%' OR genres LIKE '%Drama%'
GROUP BY 
    CASE 
        WHEN genres LIKE '%Comedy%' THEN 'Comedy'
        WHEN genres LIKE '%Drama%' THEN 'Drama'
        ELSE 'Other'
    END;