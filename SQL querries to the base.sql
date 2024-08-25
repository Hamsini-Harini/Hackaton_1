-- CREATE TABLE IF NOT EXISTS identify_duplicates (
--     URL VARCHAR (255) PRIMARY KEY,
--     Topics TEXT,
--     Visits INT,
--     Impressions INT,
--     CTR FLOAT,
--     Ranking FLOAT
-- );

-- WE need to break down comma-separated values 

-- WITH exploded_topics AS (
--   -- Step 1: Break each comma-separated list into individual topics
--   SELECT
--     url,
--     TRIM(UNNEST(STRING_TO_ARRAY(topics, ','))) AS topic
--   FROM identify_duplicates
--   WHERE topics IS NOT NULL
-- )

-- Step 2: Count the occurrences of each topic and sort by the most popular
-- SELECT
--   topic,
--   COUNT(url) AS url_count
-- FROM exploded_topics
-- GROUP BY topic
-- ORDER BY url_count DESC;

-- I realized that "Legal representation" is the most common topic, so I want to display all URLs that contain "Legal representation" as topic
 

SELECT *
FROM identify_duplicates
WHERE topics LIKE '%Legal representation%'
ORDER BY url ASC;

