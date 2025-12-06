CREATE TABLE mikhail_k_table (
    lti_user_id TEXT,
    is_correct BOOLEAN,
    attempt_type TEXT,
    created_at TIMESTAMP,
    oauth_consumer_key TEXT,
    lis_result_sourcedid TEXT,
    lis_outcome_service_url TEXT
);

SELECT * FROM mikhail_k_table;

TRUNCATE TABLE mikhail_k_table;