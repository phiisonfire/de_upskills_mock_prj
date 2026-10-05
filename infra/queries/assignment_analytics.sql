-- Run in Athena with the project Glue database selected (default:
-- movielens_movielens_t2013_daily_v2). These are analytical examples; tune
-- thresholds and date ranges for the report and state them in the submission.

-- 1) Movie ranking. A 500-rating floor trades broad coverage for less
-- small-sample volatility; compare it with the unfiltered ranking below.
WITH movie_votes AS (
    SELECT c.source_content_id, c.display_title,
           COUNT(*) AS rating_count, AVG(r.rating) AS average_rating
    FROM gold_fact_rating r
    JOIN gold_dim_content c ON c.source_content_id = r.source_content_id AND c.is_current
    WHERE NOT c.is_deleted AND c.source_content_id IS NOT NULL
    GROUP BY c.source_content_id, c.display_title
)
SELECT source_content_id, display_title, rating_count, average_rating
FROM movie_votes
WHERE rating_count >= 500
ORDER BY average_rating DESC, rating_count DESC
LIMIT 100;

-- Same calculation without the vote floor: tiny samples can dominate the top.
WITH movie_votes AS (
    SELECT c.source_content_id, c.display_title,
           COUNT(*) AS rating_count, AVG(r.rating) AS average_rating
    FROM gold_fact_rating r
    JOIN gold_dim_content c ON c.source_content_id = r.source_content_id AND c.is_current
    WHERE NOT c.is_deleted
    GROUP BY c.source_content_id, c.display_title
)
SELECT source_content_id, display_title, rating_count, average_rating
FROM movie_votes
ORDER BY average_rating DESC, rating_count DESC
LIMIT 100;

-- 2) Genre quality, popularity, and disagreement (sample variance).
SELECT g.genre, COUNT(*) AS rating_count, COUNT(DISTINCT r.source_content_id) AS movie_count,
       AVG(r.rating) AS average_rating, VAR_SAMP(r.rating) AS rating_variance
FROM gold_fact_rating r
JOIN gold_dim_content c ON c.source_content_id = r.source_content_id AND c.is_current AND NOT c.is_deleted
JOIN gold_bridge_content_genre b ON b.content_sk = c.content_sk AND NOT b.is_no_genres_listed
JOIN gold_dim_genre g ON g.genre_sk = b.genre_sk AND NOT g.is_sentinel
GROUP BY g.genre
ORDER BY rating_count DESC;

-- Highest observed rating variance by genre. Use the same grouping as above;
-- a minimum-rating threshold can be added to avoid unstable small groups.
SELECT g.genre, COUNT(*) AS rating_count, VAR_SAMP(r.rating) AS rating_variance
FROM gold_fact_rating r
JOIN gold_dim_content c ON c.content_sk = r.content_sk AND c.is_current AND NOT c.is_deleted
JOIN gold_bridge_content_genre b ON b.content_sk = c.content_sk AND NOT b.is_no_genres_listed
JOIN gold_dim_genre g ON g.genre_sk = b.genre_sk AND NOT g.is_sentinel
GROUP BY g.genre
HAVING COUNT(*) >= 500
ORDER BY rating_variance DESC
LIMIT 1;

-- 3a) Rating trend by release year.
SELECT c.release_year, COUNT(*) AS rating_count, AVG(r.rating) AS average_rating
FROM gold_fact_rating r
JOIN gold_dim_content c ON c.source_content_id = r.source_content_id AND c.is_current
WHERE c.release_year IS NOT NULL AND NOT c.is_deleted
GROUP BY c.release_year
ORDER BY c.release_year;

-- 3b) Rating trend by event month. Source event times are naive local timestamps;
-- this query does not call them UTC.
SELECT date_trunc('month', r.event_time_source) AS rating_month,
       COUNT(*) AS rating_count, AVG(r.rating) AS average_rating
FROM gold_fact_rating r
GROUP BY 1
ORDER BY 1;

-- 4a) Most frequently submitted normalized user tags.
SELECT tag_text_normalized, COUNT(*) AS submission_count,
       COUNT(DISTINCT source_party_id) AS distinct_users,
       COUNT(DISTINCT source_content_id) AS distinct_movies
FROM gold_fact_user_tag
WHERE tag_text_normalized IS NOT NULL AND trim(tag_text_normalized) <> ''
GROUP BY tag_text_normalized
ORDER BY submission_count DESC
LIMIT 100;

-- 4b) Across movies, correlate each normalized tag's submission count with
-- that movie's average rating. This is an association, not a causal effect.
WITH movie_ratings AS (
    SELECT source_content_id, AVG(rating) AS average_rating
    FROM gold_fact_rating
    GROUP BY source_content_id
), tag_movie_counts AS (
    SELECT source_content_id, tag_text_normalized, COUNT(*) AS tag_submissions
    FROM gold_fact_user_tag
    WHERE tag_text_normalized IS NOT NULL AND trim(tag_text_normalized) <> ''
    GROUP BY source_content_id, tag_text_normalized
)
SELECT t.tag_text_normalized, COUNT(*) AS movies_with_tag,
       CORR(CAST(t.tag_submissions AS DOUBLE), r.average_rating) AS tag_frequency_rating_correlation,
       AVG(r.average_rating) AS mean_movie_rating
FROM tag_movie_counts t
JOIN movie_ratings r ON r.source_content_id = t.source_content_id
GROUP BY t.tag_text_normalized
HAVING COUNT(*) >= 30
ORDER BY movies_with_tag DESC;

-- 7a) Genome coverage. Unscored movies are not assigned zero relevance.
SELECT COUNT(DISTINCT c.source_content_id) AS catalog_movies,
       COUNT(DISTINCT CASE WHEN s.source_content_id IS NOT NULL THEN c.source_content_id END) AS movies_with_genome,
       CAST(COUNT(DISTINCT CASE WHEN s.source_content_id IS NOT NULL THEN c.source_content_id END) AS DOUBLE)
         / NULLIF(COUNT(DISTINCT c.source_content_id), 0) AS genome_coverage
FROM gold_dim_content c
LEFT JOIN gold_fact_genome_score s ON s.source_content_id = c.source_content_id
WHERE c.is_current AND c.source_system = 'movielens' AND NOT c.is_deleted;

-- 7b) Example genome profile for three selected MovieLens IDs.
SELECT c.source_content_id, c.display_title, t.signal_name, s.relevance
FROM gold_fact_genome_score s
JOIN gold_dim_content c ON c.content_sk = s.content_sk AND c.is_current
JOIN gold_dim_signal_tag t ON t.signal_tag_sk = s.signal_tag_sk
WHERE c.source_content_id IN (1, 2, 3)
ORDER BY c.source_content_id, s.relevance DESC
LIMIT 60;

-- 8) Hidden-gem candidate definition: average rating >= 4.0, at least 100
-- ratings, and genome coverage below 25% of the 1,128-tag taxonomy. IMDb IDs
-- have the required tt + seven-digit format; TMDb URLs use the numeric ID.
WITH rating_summary AS (
    SELECT source_content_id, COUNT(*) AS rating_count, AVG(rating) AS average_rating
    FROM gold_fact_rating
    GROUP BY source_content_id
), genome_summary AS (
    SELECT source_content_id, COUNT(DISTINCT source_taxonomy_id) AS genome_tag_count
    FROM gold_fact_genome_score
    GROUP BY source_content_id
), current_catalog AS (
    SELECT * FROM gold_dim_content WHERE is_current AND NOT is_deleted
)
SELECT c.source_content_id, c.display_title, r.rating_count, r.average_rating,
       COALESCE(g.genome_tag_count, 0) AS genome_tag_count,
       CAST(COALESCE(g.genome_tag_count, 0) AS DOUBLE) / 1128.0 AS genome_coverage,
       MAX(CASE WHEN x.provider = 'imdb' THEN x.provider_url END) AS imdb_url,
       MAX(CASE WHEN x.provider = 'tmdb' THEN x.provider_url END) AS tmdb_url
FROM current_catalog c
JOIN rating_summary r ON r.source_content_id = c.source_content_id
LEFT JOIN genome_summary g ON g.source_content_id = c.source_content_id
LEFT JOIN gold_dim_external_content_id x ON x.source_content_id = c.source_content_id
WHERE r.rating_count >= 100 AND r.average_rating >= 4.0
  AND CAST(COALESCE(g.genome_tag_count, 0) AS DOUBLE) / 1128.0 < 0.25
GROUP BY c.source_content_id, c.display_title, r.rating_count, r.average_rating, g.genome_tag_count
ORDER BY r.average_rating DESC, r.rating_count DESC;
