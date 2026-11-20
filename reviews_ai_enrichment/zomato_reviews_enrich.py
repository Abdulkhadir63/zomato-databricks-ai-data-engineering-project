%sql

-- ============================================================
-- STEP 1: CREATE SCHEMA IF IT DOES NOT EXIST
-- ============================================================

CREATE SCHEMA IF NOT EXISTS zomato_dev.zomato_reviews_enrich;


-- ============================================================
-- STEP 2: CREATE AN EMPTY DELTA TARGET TABLE IF IT DOES NOT EXIST
-- ============================================================

CREATE TABLE IF NOT EXISTS zomato_dev.zomato_reviews_enrich.enriched_reviews
USING DELTA
AS

SELECT

    r.review_id,
    r.order_id,
    r.customer_id,
    r.restaurant_id,
    r.rating,
    r.comment,
    r.review_date,
    r.city,
    r._source_file,
    r._source_file_modification_time,
    r._bronze_ingested_at,
    r._silver_processed_at,

    CAST(NULL AS STRING) AS sentiment,
    CAST(NULL AS DOUBLE) AS confidence_score,
    CAST(NULL AS STRING) AS rationale,
    CAST(NULL AS STRING) AS classification_status,
    CAST(NULL AS STRING) AS classification_error,
    CAST(NULL AS STRING) AS content_hash,
    CAST(NULL AS STRING) AS classification_config_version,
    CAST(NULL AS STRING) AS ai_function_version,
    CAST(NULL AS TIMESTAMP) AS classified_at

FROM zomato_dev.silver.reviews r

WHERE 1 = 0;


-- ============================================================
-- STEP 3: IDENTIFY NEW REVIEWS
-- ============================================================

WITH new_reviews AS (

    SELECT r.*

    FROM zomato_dev.silver.reviews r

    LEFT ANTI JOIN zomato_dev.zomato_reviews_enrich.enriched_reviews t

        ON r.review_id = t.review_id

),


-- ============================================================
-- STEP 4: EXTRACT DISTINCT COMMENTS
-- ============================================================

distinct_comments AS (

    SELECT DISTINCT
        comment AS review_text

    FROM new_reviews

    WHERE comment IS NOT NULL
      AND TRIM(comment) <> ''

),


-- ============================================================
-- STEP 5: CLASSIFY COMMENTS USING AI
-- ============================================================

classified_comments AS (

    SELECT
        review_text,

        ai_classify(
            review_text,

            '{
              "positive": "The review expresses satisfaction or a favorable experience.",
              "negative": "The review expresses dissatisfaction or an unfavorable experience.",
              "neutral": "The review is factual or balanced without clear positive or negative sentiment.",
              "mixed": "The review clearly expresses both positive and negative sentiment."
            }',

            MAP(
                'version', '2.1',
                'enableConfidenceScores', 'true',
                'enableRationales', 'true',
                'instructions', 'Classify the overall sentiment of the customer review. Choose mixed only when both positive and negative opinions are clearly present.'
            )

        ) AS ai_result

    FROM distinct_comments

),


-- ============================================================
-- STEP 6: PARSE AI RESPONSE
-- ============================================================

parsed_comments AS (

    SELECT
        review_text,

        ai_result:response[0].value::STRING AS sentiment,

        ai_result:response[0].confidence_score::DOUBLE
            AS confidence_score,

        ai_result:response[0].rationale::STRING AS rationale,

        ai_result:error_message::STRING AS classification_error,

        ai_result:metadata.version::STRING AS ai_function_version

    FROM classified_comments

),


-- ============================================================
-- STEP 7: PREPARE MERGE SOURCE
-- ============================================================

merge_source AS (

    SELECT

        r.*,

        c.sentiment,

        c.confidence_score,

        c.rationale,

        CASE

            WHEN r.comment IS NULL OR TRIM(r.comment) = ''
                THEN 'SKIPPED'

            WHEN c.classification_error IS NOT NULL
              OR c.sentiment IS NULL
              OR c.sentiment NOT IN ('positive', 'negative', 'neutral', 'mixed')
                THEN 'FAILED'

            WHEN c.confidence_score IS NULL
              OR c.rationale IS NULL
                THEN 'PARTIAL'

            ELSE 'SUCCESS'

        END AS classification_status,

        c.classification_error,

        SHA2(r.comment, 256) AS content_hash,

        'review_sentiment_v1' AS classification_config_version,

        c.ai_function_version,

        CASE
            WHEN c.sentiment IS NOT NULL THEN CURRENT_TIMESTAMP()
        END AS classified_at

    FROM new_reviews r

    LEFT JOIN parsed_comments c

        ON r.comment = c.review_text

)


-- ============================================================
-- STEP 8: INSERT ONLY NEW REVIEW RECORDS
-- ============================================================

MERGE INTO zomato_dev.zomato_reviews_enrich.enriched_reviews AS target

USING merge_source AS source

ON target.review_id = source.review_id

WHEN NOT MATCHED THEN INSERT (

    review_id,
    order_id,
    customer_id,
    restaurant_id,
    rating,
    comment,
    review_date,
    city,
    _source_file,
    _source_file_modification_time,
    _bronze_ingested_at,
    _silver_processed_at,
    sentiment,
    confidence_score,
    rationale,
    classification_status,
    classification_error,
    content_hash,
    classification_config_version,
    ai_function_version,
    classified_at

)

VALUES (

    source.review_id,
    source.order_id,
    source.customer_id,
    source.restaurant_id,
    source.rating,
    source.comment,
    source.review_date,
    source.city,
    source._source_file,
    source._source_file_modification_time,
    source._bronze_ingested_at,
    source._silver_processed_at,
    source.sentiment,
    source.confidence_score,
    source.rationale,
    source.classification_status,
    source.classification_error,
    source.content_hash,
    source.classification_config_version,
    source.ai_function_version,
    source.classified_at

);