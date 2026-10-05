-- Run after Gold loads in Athena. Select the Glue database first. Every check
-- below should return zero rows or the stated expected relationship.

-- Fact grain: one row per rating event and tag event.
SELECT rating_event_sk, COUNT(*) AS copies
FROM gold_fact_rating
GROUP BY rating_event_sk HAVING COUNT(*) <> 1;

SELECT tag_event_sk, COUNT(*) AS copies
FROM gold_fact_user_tag
GROUP BY tag_event_sk HAVING COUNT(*) <> 1;

-- Type 2: one current version per content natural key and no overlapping
-- effective intervals. Adjacent ranges are valid: prior end == next start.
SELECT source_system, source_content_id, COUNT_IF(is_current) AS current_versions
FROM gold_dim_content
GROUP BY source_system, source_content_id
HAVING COUNT_IF(is_current) <> 1;

SELECT a.source_content_id, a.content_sk AS earlier_version, b.content_sk AS later_version
FROM gold_dim_content a
JOIN gold_dim_content b
  ON a.source_system = b.source_system
 AND a.source_content_id = b.source_content_id
 AND a.content_sk <> b.content_sk
 AND a.effective_from < b.effective_from
 AND (a.effective_to IS NULL OR a.effective_to > b.effective_from);

-- The synthetic movie 1 genre change should have at least two versions, and
-- the prior version should end exactly when the new one starts.
SELECT source_content_id, version, genres_raw, effective_from, effective_to,
       is_current, scd_change_type
FROM gold_dim_content
WHERE source_system = 'movielens' AND source_content_id = 1
ORDER BY effective_from;

-- Type 3 is one-step history: current and previous display titles are both
-- retained for the synthetic movie 2 title-change scenario.
SELECT source_content_id, display_title, previous_display_title, title_changed_at
FROM gold_dim_content
WHERE source_system = 'movielens' AND source_content_id = 2 AND is_current;

-- Type 1 technical title correction should not create another Type 2 version.
SELECT source_content_id, COUNT(*) AS versions, MAX(display_title) AS current_display_title
FROM gold_dim_content
WHERE source_system = 'movielens' AND source_content_id = 25936
GROUP BY source_content_id;

-- Point-in-time versus current-state genre classification. Rows with a
-- difference demonstrate why a current-only join cannot answer historical
-- questions. Expect MovieLens movie 1 ratings before 2015-01-01 12:00:00 to
-- differ if its Type 2 genre update was loaded.
WITH pit AS (
    SELECT r.rating_event_sk, r.source_content_id, ARRAY_SORT(ARRAY_AGG(DISTINCT b.genre)) AS genres_at_rating
    FROM gold_fact_rating r
    JOIN gold_dim_content c
      ON c.source_system = r.source_system
     AND c.source_content_id = r.source_content_id
     AND c.effective_from <= r.event_time_source
     AND (c.effective_to IS NULL OR r.event_time_source < c.effective_to)
    LEFT JOIN gold_bridge_content_genre b ON b.content_sk = c.content_sk AND NOT b.is_no_genres_listed
    WHERE r.source_content_id = 1
    GROUP BY r.rating_event_sk, r.source_content_id
), current_state AS (
    SELECT c.source_content_id, ARRAY_SORT(ARRAY_AGG(DISTINCT b.genre)) AS current_genres
    FROM gold_dim_content c
    LEFT JOIN gold_bridge_content_genre b ON b.content_sk = c.content_sk AND NOT b.is_no_genres_listed
    WHERE c.source_system = 'movielens' AND c.source_content_id = 1 AND c.is_current
    GROUP BY c.source_content_id
)
SELECT p.rating_event_sk, p.source_content_id, p.genres_at_rating, c.current_genres
FROM pit p JOIN current_state c USING (source_content_id)
WHERE p.genres_at_rating IS DISTINCT FROM c.current_genres
LIMIT 100;

-- URL formatting checks. IMDb URLs should use tt plus seven digits; TMDb IDs
-- should be numeric at the end of the URL.
SELECT * FROM gold_dim_external_content_id
WHERE (provider = 'imdb' AND NOT regexp_like(provider_url, '^https://www[.]imdb[.]com/title/tt[0-9]{7}/$'))
   OR (provider = 'tmdb' AND NOT regexp_like(provider_url, '^https://www[.]themoviedb[.]org/movie/[0-9]+$'));

-- Genome fact keys should be unique at one source content x taxonomy tag.
SELECT source_content_id, source_taxonomy_id, COUNT(*) AS copies
FROM gold_fact_genome_score
GROUP BY source_content_id, source_taxonomy_id HAVING COUNT(*) <> 1;
