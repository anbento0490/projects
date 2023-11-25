CREATE EXTENSION pgcrypto;

-- Insert data using generate_series() and random values
INSERT INTO transactions (transaction_id, user_id, product_name, transaction_date, amount_gbp)
SELECT
    CAST(10000000 + floor(random() * 90000000) AS bigint)   as transaction_id,
    gen_random_uuid ()                                      as user_id,
    CASE
        WHEN random() < 0.33 THEN 'TRANSFER'
        WHEN random() < 0.66 THEN 'CARD_TRANSACTION'
        ELSE 'DIRECT_DEBIT'
    END                                                     as product_name,
    now() - random() * INTERVAL '10 days'                   as transaction_date,
    RANDOM()*(1000 - 500) + 500                             as amount_gbp
FROM generate_series(1, 5000000) s(i);
