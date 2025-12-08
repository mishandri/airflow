CREATE TABLE mikhail_k_agg_table_weekly(
    lti_user_id TEXT,
    attempt_type TEXT,
    cnt_attempt INTEGER,
    cnt_correct INTEGER,
    date timestamp
);

SELECT * FROM mikhail_k_agg_table_weekly;

TRUNCATE TABLE mikhail_k_agg_table_weekly;