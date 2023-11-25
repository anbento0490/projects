-- Create the transactions table
CREATE TABLE transactions (
    transaction_id   INT,
    user_id          VARCHAR,
    product_name     VARCHAR,
    transaction_date DATE,
    amount_gbp       DECIMAL(35,2)
);
