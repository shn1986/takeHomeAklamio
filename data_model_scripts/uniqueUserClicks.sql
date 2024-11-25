CREATE TABLE IF NOT EXISTS "Akalamio_TakeHome".unique_user_clicks (
    customer_id INT NOT NULL,
    date_hour TIMESTAMP NOT NULL,
    unique_user_clicks INT DEFAULT 0,
    PRIMARY KEY (customer_id, date_hour)
);

CREATE INDEX IF NOT EXISTS idx_unique_user_clicks_customer_hour ON "Akalamio_TakeHome".unique_user_clicks (customer_id, date_hour);